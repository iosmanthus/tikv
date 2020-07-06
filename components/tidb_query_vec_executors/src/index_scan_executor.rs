// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tidb_query_datatype::EvalType;
use tipb::ColumnInfo;
use tipb::FieldType;
use tipb::IndexScan;

use super::util::scan_executor::*;
use crate::interface::*;
use codec::prelude::NumberDecoder;
use tidb_query_common::storage::{IntervalRange, Storage};
use tidb_query_common::Result;
use tidb_query_datatype::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use tidb_query_datatype::codec::table::{check_index_key, MAX_OLD_ENCODED_VALUE_LEN};
use tidb_query_datatype::codec::{datum, table};
use tidb_query_datatype::expr::{EvalConfig, EvalContext};

pub struct BatchIndexScanExecutor<S: Storage>(ScanExecutor<S, IndexScanExecutorImpl>);

// We assign a dummy type `Box<dyn Storage<Statistics = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchIndexScanExecutor<Box<dyn Storage<Statistics = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &IndexScan) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage> BatchIndexScanExecutor<S> {
    pub fn new(
        storage: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        primary_column_ids_len: usize,
        is_backward: bool,
        unique: bool,
    ) -> Result<Self> {
        // Note 1: `unique = true` doesn't completely mean that it is a unique index scan. Instead
        // it just means that we can use point-get for this index. In the following scenarios
        // `unique` will be `false`:
        // - scan from a non-unique index
        // - scan from a unique index with like: where unique-index like xxx
        //
        // Note 2: Unlike table scan executor, the accepted `columns_info` of index scan executor is
        // strictly stipulated. The order of columns in the schema must be the same as index data
        // stored and if PK handle is needed it must be placed as the last one.
        //
        // Note 3: Currently TiDB may send multiple PK handles to TiKV (but only the last one is
        // real). We accept this kind of request for compatibility considerations, but will be
        // forbidden soon.
        use DecodeHandleStrategy::*;

        let is_int_handle = columns_info.last().map_or(false, |ci| ci.get_pk_handle());
        let is_common_handle = primary_column_ids_len > 0;
        let (decode_strategy, handle_column_cnt) = match (is_int_handle, is_common_handle) {
            (false, false) => (NoDecode, 0),
            (false, true) => (DecodeCommonHandle, primary_column_ids_len),
            (true, false) => (DecodeIntHandle, 1),
            // TiDB may accidentally push down both int handle or common handle.
            // However, we still try to decode int handle.
            _ => {
                warn!("Both int handle and common handle are push downed");
                (DecodeIntHandle, 1)
            }
        };

        if handle_column_cnt > columns_info.len() {
            return Err(other_err!(
                "The number of handle columns exceeds the length of `columns_info`"
            ));
        }

        let schema: Vec<_> = columns_info
            .iter()
            .map(|ci| field_type_from_column_info(&ci))
            .collect();

        let columns_id_without_handle: Vec<_> = columns_info
            [..columns_info.len() - handle_column_cnt]
            .iter()
            .map(|ci| ci.get_column_id())
            .collect();

        let imp = IndexScanExecutorImpl {
            context: EvalContext::new(config),
            schema,
            columns_id_without_handle,
            decode_strategy,
        };
        let wrapper = ScanExecutor::new(ScanExecutorOptions {
            imp,
            storage,
            key_ranges,
            is_backward,
            is_key_only: false,
            accept_point_range: unique,
        })?;
        Ok(Self(wrapper))
    }
}

impl<S: Storage> BatchExecutor for BatchIndexScanExecutor<S> {
    type StorageStats = S::Statistics;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows)
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.0.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.0.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.0.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.0.can_be_cached()
    }
}

#[derive(PartialEq, Debug)]
enum DecodeHandleStrategy {
    NoDecode,
    DecodeIntHandle,
    DecodeCommonHandle,
}

struct IndexScanExecutorImpl {
    /// See `TableScanExecutorImpl`'s `context`.
    context: EvalContext,

    /// See `TableScanExecutorImpl`'s `schema`.
    schema: Vec<FieldType>,

    /// ID of interested columns (exclude PK handle column).
    columns_id_without_handle: Vec<i64>,

    /// The strategy to decode handles.
    /// Handle will be always placed in the last column.
    decode_strategy: DecodeHandleStrategy,
}

impl ScanExecutorImpl for IndexScanExecutorImpl {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    /// Constructs empty columns, with PK containing int handle in decoded format and the rest in raw format.
    ///
    /// Note: the structure of the constructed column is the same as table scan executor but due
    /// to different reasons.
    fn build_column_vec(&self, scan_rows: usize) -> LazyBatchColumnVec {
        use DecodeHandleStrategy::*;

        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);

        for _ in 0..self.columns_id_without_handle.len() {
            columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
        }

        match self.decode_strategy {
            NoDecode => {}
            DecodeIntHandle => {
                columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                    scan_rows,
                    EvalType::Int,
                ));
            }
            DecodeCommonHandle => {
                for _ in self.columns_id_without_handle.len()..columns_len {
                    columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
                }
            }
        }

        assert_eq!(columns.len(), columns_len);
        LazyBatchColumnVec::from(columns)
    }

    // Currently, we have 6 foramts of index value.
    // Value layout:
    // +--With Restore Data(for indices on string columns)
    // |  |
    // |  +--Non Unique (TailLen = len(PaddingData) + len(Flag), TailLen < 8 always)
    // |  |  |
    // |  |  |  Layout: TailLen |      RestoreData  |      PaddingData
    // |  |  |  Length: 1       | size(RestoreData) | size(paddingData)
    // |  |  |
    // |  |  |  The length >= 10 always because of padding.
    // |  |
    // |  |
    // |  +--Unique Common Handle
    // |  |  |
    // |  |  |
    // |  |  |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle       | RestoreData
    // |  |  |  Length: 1    | 1            | 2           | size(CHandle) | size(RestoreData)
    // |  |  |
    // |  |  |  The length > 10 always because of CHandle size.
    // |  |
    // |  |
    // |  +--Unique Integer Handle (TailLen = len(Handle) + len(Flag), TailLen == 8 || TailLen == 9)
    // |     |
    // |     |  Layout: 0x08 |    RestoreData    |  Handle
    // |     |  Length: 1    | size(RestoreData) |   8
    // |     |
    // |     |  The length >= 10 always since size(RestoreData) > 0.
    // |
    // |
    // +--Without Restore Data
    // |
    // +--Non Unique
    // |  |
    // |  |  Layout: '0'
    // |  |  Length:  1
    // |
    // +--Unique Common Handle
    // |  |
    // |  |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle
    //    |  Length: 1    | 1            | 2           | size(CHandle)
    // |
    // |
    // +--Unique Integer Handle
    // |
    // |  Layout: Handle
    // |  Length:   8
    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        check_index_key(key)?;
        if value.len() > MAX_OLD_ENCODED_VALUE_LEN {
            if value[0] <= 1 && value[1] == table::INDEX_COMMON_HANDLE_FLAG {
                self.process_unique_common_handle_value(key, value, columns)
            } else {
                self.process_normal_new_collation_value(key, value, columns)
            }
        } else {
            self.process_normal_old_collation_value(key, value, columns)
        }
    }
}

impl IndexScanExecutorImpl {
    #[inline]
    fn decode_handle_from_value(&self, mut value: &[u8]) -> Result<i64> {
        // NOTE: it is not `number::decode_i64`.
        value
            .read_u64()
            .map_err(|_| other_err!("Failed to decode handle in value as i64"))
            .map(|x| x as i64)
    }

    #[inline]
    fn decode_handle_from_key(&self, key: &[u8]) -> Result<i64> {
        let flag = key[0];
        let mut val = &key[1..];

        // TODO: Better to use `push_datum`. This requires us to allow `push_datum`
        // receiving optional time zone first.

        match flag {
            datum::INT_FLAG => val
                .read_i64()
                .map_err(|_| other_err!("Failed to decode handle in key as i64")),
            datum::UINT_FLAG => val
                .read_u64()
                .map_err(|_| other_err!("Failed to decode handle in key as u64"))
                .map(|x| x as i64),
            _ => Err(other_err!("Unexpected handle flag {}", flag)),
        }
    }

    fn extract_columns_from_row_format(
        &mut self,
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use tidb_query_datatype::codec::row::v2::{RowSlice, V1CompatibleEncoder};

        let row = RowSlice::from_bytes(value)?;
        for (idx, col_id) in self.columns_id_without_handle.iter().enumerate() {
            if let Some((start, offset)) = row.search_in_non_null_ids(*col_id)? {
                let mut buffer_to_write = columns[idx].mut_raw().begin_concat_extend();
                buffer_to_write
                    .write_v2_as_datum(&row.values()[start..offset], &self.schema[idx])?;
            } else if row.search_in_null_ids(*col_id) {
                columns[idx].mut_raw().push(datum::DATUM_DATA_NULL);
            } else {
                return Err(other_err!("Unexpected missing column {}", col_id));
            }
        }
        Ok(())
    }

    fn extract_columns_from_common_handle(
        payload: &mut &[u8],
        columns: &mut [LazyBatchColumn],
    ) -> Result<()> {
        Self::extract_columns_from_datum_format(payload, columns)?;
        // Skip the zero padding.
        while !payload.is_empty() && payload[0] == 0 {
            *payload = &payload[1..];
        }
        Ok(())
    }

    fn extract_columns_from_datum_format(
        datum: &mut &[u8],
        columns: &mut [LazyBatchColumn],
    ) -> Result<()> {
        for column in columns {
            if datum.is_empty() {
                return Err(other_err!("Value is missing some columns"));
            }
            let (value, remaining) = datum::split_datum(datum, false)?;
            column.mut_raw().push(value);
            *datum = remaining;
        }
        Ok(())
    }

    fn process_unique_common_handle_value(
        &mut self,
        key: &[u8],
        mut value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use DecodeHandleStrategy::*;

        let tail_len = value[0] as usize;
        let handle_len = ((value[2] as usize) << 8) + value[3] as usize;
        let handle_end_offset = 4 + handle_len;

        // Strip the tail.
        value = &value[..value.len() - tail_len];
        // If there are some restore data.
        if handle_end_offset < value.len() {
            let restore_values = &value[handle_end_offset..];
            self.extract_columns_from_row_format(restore_values, columns)?;
        } else {
            // The datum payload part of the key.
            let mut key_payload = &key[table::PREFIX_LEN + table::ID_LEN..];
            Self::extract_columns_from_datum_format(
                &mut key_payload,
                &mut columns[0..self.columns_id_without_handle.len()],
            )?;
        }

        if let DecodeCommonHandle = self.decode_strategy {
            let mut common_handle = &value[4..handle_end_offset];
            Self::extract_columns_from_common_handle(
                &mut common_handle,
                &mut columns[self.columns_id_without_handle.len()..self.schema.len()],
            )?;
        }

        Ok(())
    }

    fn process_normal_new_collation_value(
        &mut self,
        mut key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use DecodeHandleStrategy::*;
        let tail_len = value[0] as usize;
        let restore_values = &value[1..value.len() - tail_len];
        self.extract_columns_from_row_format(restore_values, columns)?;

        match self.decode_strategy {
            NoDecode => {}
            _ => {
                if tail_len < 8 {
                    if self.decode_strategy != DecodeCommonHandle {
                        return Err(other_err!(
                            "Corrupted value encountered, the tail length of index value with common handles are always less than 8"
                        ));
                    }

                    key = &key[table::PREFIX_LEN + table::ID_LEN..];
                    datum::skip_n(&mut key, self.columns_id_without_handle.len())?;
                    Self::extract_columns_from_common_handle(
                        &mut key,
                        &mut columns[self.columns_id_without_handle.len()..self.schema.len()],
                    )?;
                } else {
                    if self.decode_strategy != DecodeIntHandle {
                        return Err(other_err!("Corrupted value encountered, the tail length of index values with int handles are always no less than 8"));
                    }
                    let handle = self.decode_handle_from_value(value)?;
                    columns[self.columns_id_without_handle.len()]
                        .mut_decoded()
                        .push_int(Some(handle));
                }
            }
        }
        Ok(())
    }

    fn process_normal_old_collation_value(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use DecodeHandleStrategy::*;

        // The payload part of the key
        let mut key_payload = &key[table::PREFIX_LEN + table::ID_LEN..];

        Self::extract_columns_from_datum_format(
            &mut key_payload,
            &mut columns[0..self.columns_id_without_handle.len()],
        )?;

        match self.decode_strategy {
            NoDecode => {}
            _ => {
                // For normal index, it is placed at the end and any columns prior to it are
                // ensured to be interested. For unique index, it is placed in the value.
                if key_payload.is_empty() {
                    // This is a unique index, and we should look up PK handle in value.
                    let handle_val = self.decode_handle_from_value(value)?;
                    columns[self.columns_id_without_handle.len()]
                        .mut_decoded()
                        .push_int(Some(handle_val));
                } else {
                    // This is a normal index. The remaining key payload part is the PK handle.
                    // Let's decode it and put in the column.
                    if let DecodeIntHandle = self.decode_strategy {
                        let handle_val = self.decode_handle_from_key(key_payload)?;
                        columns[self.columns_id_without_handle.len()]
                            .mut_decoded()
                            .push_int(Some(handle_val));
                    } else {
                        Self::extract_columns_from_common_handle(
                            &mut key_payload,
                            &mut columns[self.columns_id_without_handle.len()..self.schema.len()],
                        )?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use codec::prelude::NumberEncoder;
    use kvproto::coprocessor::KeyRange;
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use tipb::ColumnInfo;

    use tidb_query_common::storage::test_fixture::FixtureStorage;
    use tidb_query_common::util::convert_to_prefix_next;
    use tidb_query_datatype::codec::data_type::*;
    use tidb_query_datatype::codec::{datum, table, Datum};
    use tidb_query_datatype::expr::EvalConfig;

    #[test]
    fn test_basic() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let mut ctx = EvalContext::default();

        // Index schema: (INT, FLOAT)

        // the elements in data are: [int index, float index, handle id].
        let data = vec![
            [Datum::I64(-5), Datum::F64(0.3), Datum::I64(10)],
            [Datum::I64(5), Datum::F64(5.1), Datum::I64(5)],
            [Datum::I64(5), Datum::F64(10.5), Datum::I64(2)],
        ];

        // The column info for each column in `data`. Used to build the executor.
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        // Case 1. Normal index.

        // For a normal index, the PK handle is stored in the key and nothing interesting is stored
        // in the value. So let's build corresponding KV data.

        let store = {
            let kv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, datums).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    let value = vec![];
                    (key, value)
                })
                .collect();
            FixtureStorage::from(kv)
        };

        {
            // Case 1.1. Normal index, without PK, scan total index in reverse order.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::Min]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::Max]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[0].clone(), columns_info[1].clone()],
                key_ranges,
                0,
                true,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 3);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(5), Some(5), Some(-5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_real_slice(),
                &[
                    Real::new(10.5).ok(),
                    Real::new(5.1).ok(),
                    Real::new(0.3).ok()
                ]
            );
        }

        {
            // Case 1.2. Normal index, with PK, scan index prefix.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::I64(2)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::I64(6)]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                0,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_real_slice(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().as_int_slice(),
                &[Some(5), Some(2)]
            );
        }

        // Case 2. Unique index.

        // For a unique index, the PK handle is stored in the value.

        let store = {
            let kv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, &datums[0..2]).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    // PK handle in the value
                    let mut value = vec![];
                    value
                        .write_u64(datums[2].as_int().unwrap().unwrap() as u64)
                        .unwrap();
                    (key, value)
                })
                .collect();
            FixtureStorage::from(kv)
        };

        {
            // Case 2.1. Unique index, prefix range scan.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::I64(5)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                0,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_real_slice(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().as_int_slice(),
                &[Some(5), Some(2)]
            );
        }

        {
            // Case 2.2. Unique index, point scan.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data =
                    datum::encode_key(&mut ctx, &[Datum::I64(5), Datum::F64(5.1)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                0,
                false,
                true,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 1);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_real_slice(),
                &[Real::new(5.1).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().as_int_slice(),
                &[Some(5)]
            );
        }
    }
}
