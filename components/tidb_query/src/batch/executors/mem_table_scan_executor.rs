use crate::batch::interface::*;
use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::codec::datum::{self, DatumEncoder};
use crate::codec::Datum;
use crate::expr::{EvalConfig, EvalContext};
use crate::storage::{IntervalRange, Storage};
use crate::Result;

use super::util::mem_scan_executor::{MemScanExecutor, SysInfoCollector};
use super::util::scan_executor::{check_columns_info_supported, field_type_from_column_info};

use sysinfo::{NetworkExt, ProcessorExt, SystemExt};
use tipb::{ColumnInfo, FieldType, MemTableScan};

use std::marker::PhantomData;
use std::sync::Arc;

pub struct BatchMemTableScanExecutor<S: Storage> {
    store_id: u64,
    context: EvalContext,
    schema: Vec<FieldType>,
    table_name: String,
    phantom: PhantomData<S>,
}

impl BatchMemTableScanExecutor<Box<dyn Storage<Statistics=()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &MemTableScan) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage> BatchMemTableScanExecutor<S> {
    pub fn new(
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        store_id: u64,
        table_name: String,
    ) -> Result<Self> {
        let context = EvalContext::new(config);
        let mut schema = Vec::new();

        for column_info in columns_info.iter() {
            if column_info.get_pk_handle() {
                return Err(box_err!("Memory table are not allowed primary key now"));
            }
            schema.push(field_type_from_column_info(column_info));
        }
        Ok(Self {
            store_id,
            context,
            schema,
            table_name,
            phantom: PhantomData,
        })
    }
}

impl<S: Storage> MemScanExecutor for BatchMemTableScanExecutor<S> {
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    fn build_column_vec(&self, rows: usize) -> LazyBatchColumnVec {
        LazyBatchColumnVec::from(vec![
            LazyBatchColumn::raw_with_capacity(rows);
            self.schema.len()
        ])
    }

    fn process_row(&mut self, columns: &mut LazyBatchColumnVec) -> Result<()> {

        // TODO: Fill the table with push down schema.
        let datums = match self.table_name.as_str() {
            "TIKV_SERVER_STATS_INFO_CLUSTER" => ServerStatInfo::new(self.store_id).collect()?,
            "TIKV_SERVER_NET_STATS_INFO_CLUSTER" => ServerNetInfo::new(self.store_id).collect()?,
            _ => return Err(box_err!("memory table `{}` is not supported yet.", self.table_name)),
        };
        assert_eq!(columns.columns_len(), datums.len());

        for datum in datums {
            let mut value = Vec::new();
            value.write_datum(&datum, false)?;
            let mut remaining = &value[..];
            let mut index = 0;
            while !remaining.is_empty() {
                let (val, rest) = datum::split_datum(remaining, false)?;
                columns[index].mut_raw().push(val);
                remaining = rest;
                index += 1;
            }
        }

        Ok(())
    }
}

struct ServerStatInfo {
    store_id: u64,
}

impl ServerStatInfo {
    pub fn new(store_id: u64) -> Self {
        Self {
            store_id
        }
    }
}

impl SysInfoCollector for ServerStatInfo {
    fn collect(&self) -> Result<Vec<Vec<Datum>>> {
        let mut system = sysinfo::System::new();
        system.refresh_all();

        let ip = machine_ip::get().unwrap().to_string().into_bytes();
        let processor_list = system.get_processor_list();
        let cpu_usage = processor_list
            .iter()
            .map(|processor| f64::from(processor.get_cpu_usage()))
            .sum::<f64>()
            / (processor_list.len() as f64);
        let total_memory = system.get_total_memory();
        let used_memory = system.get_used_memory();
        let node_id = format!("tikv{}", self.store_id).into_bytes();

        let datums = vec![
            Datum::Bytes(ip),
            Datum::F64(cpu_usage),
            Datum::U64(total_memory),
            Datum::U64(used_memory),
            Datum::Bytes(node_id),
        ];

        Ok(vec![datums])
    }
}

struct ServerNetInfo {
    store_id: u64,
}

impl ServerNetInfo {
    pub fn new(store_id: u64) -> Self {
        ServerNetInfo {
            store_id
        }
    }
}

impl SysInfoCollector for ServerNetInfo {
    fn collect(&self) -> Result<Vec<Vec<Datum>>> {
        let mut system = sysinfo::System::new();
        system.refresh_all();

        let ip = machine_ip::get().unwrap().to_string().into_bytes();
        let bytes_in = system.get_network().get_income();
        let bytes_out = system.get_network().get_outcome();
        let node_id = format!("tikv{}", self.store_id).into_bytes();

        let datums = vec![
            Datum::Bytes(ip),
            Datum::Bytes(b"eth0".to_vec()),
            Datum::U64(bytes_out),
            Datum::U64(bytes_in),
            Datum::Bytes(node_id)
        ];

        Ok(vec![datums])
    }
}

impl<S: Storage> BatchExecutor for BatchMemTableScanExecutor<S> {
    type StorageStats = S::Statistics;

    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    fn next_batch(&mut self, _: usize) -> BatchExecuteResult {
        let mut columns = self.build_column_vec(1);
        let _ = self.process_row(&mut columns);
        columns.truncate_into_equal_length();

        let logical_rows = (0..columns.rows_len()).collect();
        BatchExecuteResult {
            physical_columns: columns,
            logical_rows,
            is_drained: Ok(true),
            warnings: self.context.take_warnings(),
        }
    }

    fn collect_exec_stats(&mut self, _: &mut ExecuteStats) {}

    fn take_scanned_range(&mut self) -> IntervalRange {
        IntervalRange {
            lower_inclusive: vec![],
            upper_exclusive: vec![],
        }
    }

    fn collect_storage_stats(&mut self, _: &mut Self::StorageStats) {}
}
