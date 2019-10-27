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
use rand::prelude::*;
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
        warn!("{}", self.table_name);
        let datums = match self.table_name.to_uppercase().as_str() {
            "TIKV_SERVER_STATS_INFO_CLUSTER" => ServerStatInfo::new(self.store_id).collect(),
            "TIKV_SERVER_NET_STATS_INFO_CLUSTER" => ServerNetInfo::new(self.store_id).collect(),
            _ => return Err(box_err!("memory table `{}` is not supported yet.", self.table_name)),
        };

        let mut value = Vec::new();
        value.write_datum(&datums, false)?;
        let mut remaining = &value[..];
        let mut index = 0;
        while !remaining.is_empty() {
            let (val, rest) = datum::split_datum(remaining, false)?;
            columns[index].mut_raw().push(val);
            remaining = rest;
            index += 1;
        }

        Ok(())
    }
}

fn gen_ip() -> Vec<u8> {
    let mut ip = String::from("117");
    for _ in 0..3 {
        ip.push_str(&format!(".{}", thread_rng().gen_range(1, 256)));
    }
    ip.into_bytes()
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
    fn collect(&self) -> Vec<Datum> {
        let mut system = sysinfo::System::new();
        system.refresh_all();

        let processor_list = system.get_processor_list();
        let cpu_usage = processor_list
            .iter()
            .map(|processor| f64::from(processor.get_cpu_usage()))
            .sum::<f64>()
            / (processor_list.len() as f64);
        let used_memory = system.get_used_memory() as f64 / system.get_total_memory() as f64;
        let node_id = format!("tikv{}", self.store_id).into_bytes();

        vec![
            Datum::Bytes(gen_ip()),
            Datum::F64(cpu_usage),
            Datum::F64(used_memory),
            Datum::Bytes(node_id),
        ]
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
    fn collect(&self) -> Vec<Datum> {
        let mut system = sysinfo::System::new();
        system.refresh_all();

        let bytes_in = system.get_network().get_income();
        let bytes_out = system.get_network().get_outcome();
        let node_id = format!("tikv{}", self.store_id).into_bytes();

        vec![
            Datum::Bytes(gen_ip()),
            Datum::Bytes(b"eth0".to_vec()),
            Datum::U64(bytes_out),
            Datum::U64(bytes_in),
            Datum::Bytes(node_id)
        ]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::fixture::FixtureStorage;

    #[test]
    fn test_batch_mem_table_scan_executor() {
        let mut mem_table_scan_executor = BatchMemTableScanExecutor::<FixtureStorage>::new(Arc::new(EvalConfig::default_for_test()), vec![ColumnInfo::default(); 4], 1, "TIKV_SERVER_STATS_INFO_CLUSTER".to_string()).unwrap();
        assert_eq!(mem_table_scan_executor.next_batch(1).physical_columns.columns_len(), 4);
    }
}
