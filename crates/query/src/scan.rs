//! The `topics` table: manifest pruning, then per file row-group statistics, bloom filters
//! and a row filter on header pivot columns.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Int32Type, Int64Type, Schema, SchemaRef, TimestampMillisecondType};
use arrow::error::ArrowError;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use model::schema::{self, base_schema, pivot_column};
use object_store::path::Path;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter,
};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use table::{AddSegment, DataFormat, Snapshot};
use uuid::Uuid;

use crate::cache::FileCache;
use crate::error::{Error, Result};
use crate::predicates::{Predicate, TopicFilter};

#[derive(Debug, Default)]
pub struct ScanMetrics {
    pub files_total: AtomicU64,
    pub files_pruned: AtomicU64,
    pub files_read: AtomicU64,
    pub row_groups_total: AtomicU64,
    pub row_groups_pruned_stats: AtomicU64,
    pub row_groups_pruned_bloom: AtomicU64,
}

struct CachedParquetReader {
    cache: Arc<FileCache>,
    id: Uuid,
    path: Path,
    size: u64,
}

fn parquet_err(e: Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

impl AsyncFileReader for CachedParquetReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let cache = self.cache.clone();
        let (id, path) = (self.id, self.path.clone());
        async move {
            cache
                .get_range(&id, &path, range)
                .await
                .map_err(parquet_err)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            if let Some(m) = self.cache.cached_metadata(&self.id) {
                return Ok(m);
            }
            let size = self.size;
            let m = Arc::new(
                ParquetMetaDataReader::new()
                    .load_and_finish(&mut *self, size)
                    .await?,
            );
            self.cache.put_metadata(self.id, m.clone());
            Ok(m)
        }
        .boxed()
    }
}

pub(crate) struct ScanContext {
    pub cache: Arc<FileCache>,
    pub root: Path,
    pub pred: Predicate,
    pub schema: SchemaRef,
    pub metrics: Arc<ScanMetrics>,
}

impl fmt::Debug for ScanContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanContext")
            .field("pred", &self.pred)
            .finish()
    }
}

impl ScanContext {
    fn path_of(&self, rel: &str) -> Path {
        rel.split('/')
            .filter(|p| !p.is_empty())
            .fold(self.root.clone(), |p, part| p.join(part))
    }
}

/// Selects `schema`'s columns (by name) from `batch`, keeping the row count.
fn project_to(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        return Ok(RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(model::Error::from)?);
    }
    Ok(model::conform(batch, schema)?)
}

async fn scan_segment(ctx: &ScanContext, seg: &AddSegment) -> Result<Vec<RecordBatch>> {
    ctx.metrics.files_read.fetch_add(1, Ordering::Relaxed);
    let path = ctx.path_of(&seg.path);
    match seg.format {
        DataFormat::Avro => {
            let data = ctx
                .cache
                .get_range(&seg.segment_id, &path, 0..seg.size)
                .await?;
            model::read_file(model::FileFormat::Avro, data)?
                .iter()
                .map(|b| project_to(b, &ctx.schema))
                .collect()
        }
        DataFormat::Parquet => scan_parquet(ctx, seg, path).await,
    }
}

fn stats_column(
    meta: &ParquetMetaData,
    file_schema: &Schema,
    column: &str,
) -> Option<(ArrayRef, ArrayRef)> {
    let conv =
        StatisticsConverter::try_new(column, file_schema, meta.file_metadata().schema_descr())
            .ok()?;
    let mins = conv.row_group_mins(meta.row_groups().iter()).ok()?;
    let maxs = conv.row_group_maxes(meta.row_groups().iter()).ok()?;
    Some((mins, maxs))
}

/// Row groups whose statistics may satisfy the predicate.
fn prune_row_groups(pred: &Predicate, meta: &ParquetMetaData, file_schema: &Schema) -> Vec<bool> {
    let n = meta.num_row_groups();
    let mut keep = vec![true; n];
    if !pred.topics.is_empty()
        && let Some((mins, maxs)) = stats_column(meta, file_schema, schema::TOPIC)
    {
        let (mins, maxs) = (mins.as_string::<i32>(), maxs.as_string::<i32>());
        for (i, k) in keep.iter_mut().enumerate() {
            if mins.is_null(i) || maxs.is_null(i) {
                continue;
            }
            let (lo, hi) = (mins.value(i), maxs.value(i));
            *k &= pred.topics.iter().all(|f| match f {
                TopicFilter::Any(set) => set.iter().any(|t| lo <= t.as_str() && t.as_str() <= hi),
                TopicFilter::Prefix(p) => {
                    lo.starts_with(p.as_str())
                        || hi.starts_with(p.as_str())
                        || (lo < p.as_str() && hi > p.as_str())
                }
            });
        }
    }
    if let Some(parts) = &pred.partitions
        && let Some((mins, maxs)) = stats_column(meta, file_schema, schema::PARTITION)
    {
        let (mins, maxs) = (
            mins.as_primitive::<Int32Type>(),
            maxs.as_primitive::<Int32Type>(),
        );
        for (i, k) in keep.iter_mut().enumerate() {
            if !mins.is_null(i) && !maxs.is_null(i) {
                *k &= parts
                    .iter()
                    .any(|p| mins.value(i) <= *p && *p <= maxs.value(i));
            }
        }
    }
    let mut range = |column: &str, lo: Option<i64>, hi: Option<i64>, ts: bool| {
        if lo.is_none() && hi.is_none() {
            return;
        }
        let Some((mins, maxs)) = stats_column(meta, file_schema, column) else {
            return;
        };
        for (i, k) in keep.iter_mut().enumerate() {
            if mins.is_null(i) || maxs.is_null(i) {
                continue;
            }
            let (min, max) = if ts {
                (
                    mins.as_primitive::<TimestampMillisecondType>().value(i),
                    maxs.as_primitive::<TimestampMillisecondType>().value(i),
                )
            } else {
                (
                    mins.as_primitive::<Int64Type>().value(i),
                    maxs.as_primitive::<Int64Type>().value(i),
                )
            };
            *k &= lo.is_none_or(|l| max >= l) && hi.is_none_or(|h| min <= h);
        }
    };
    range(schema::OFFSET, pred.offset_min, pred.offset_max, false);
    range(schema::TIMESTAMP, pred.ts_min, pred.ts_max, true);
    keep
}

fn leaf_index(meta: &ParquetMetaData, column: &str) -> Option<usize> {
    meta.file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.path().string() == column)
}

async fn scan_parquet(ctx: &ScanContext, seg: &AddSegment, path: Path) -> Result<Vec<RecordBatch>> {
    let mut reader = CachedParquetReader {
        cache: ctx.cache.clone(),
        id: seg.segment_id,
        path,
        size: seg.size,
    };
    let meta = reader
        .get_metadata(None)
        .await
        .map_err(model::Error::from)?;
    let arrow_meta = ArrowReaderMetadata::try_new(meta.clone(), ArrowReaderOptions::new())
        .map_err(model::Error::from)?;
    let file_schema = arrow_meta.schema().clone();
    let descr = meta.file_metadata().schema_descr_ptr();

    let n = meta.num_row_groups();
    ctx.metrics
        .row_groups_total
        .fetch_add(n as u64, Ordering::Relaxed);
    let mut keep = prune_row_groups(&ctx.pred, &meta, &file_schema);
    let after_stats = keep.iter().filter(|k| **k).count();
    ctx.metrics
        .row_groups_pruned_stats
        .fetch_add((n - after_stats) as u64, Ordering::Relaxed);

    // equality predicates that can use bloom filters: (leaf column, accepted values)
    let mut bloom_checks: Vec<(usize, &Vec<Vec<u8>>)> = Vec::new();
    // pivot predicates evaluated exactly per row: (column name, accepted values)
    let mut pivot_filters: Vec<(String, Vec<Vec<u8>>)> = Vec::new();
    for (key, values) in &ctx.pred.headers {
        if seg.pivot_keys.iter().any(|k| k == key) {
            let col = pivot_column(key);
            if let Some(leaf) = leaf_index(&meta, &col) {
                bloom_checks.push((leaf, values));
            }
            pivot_filters.push((col, values.clone()));
        }
    }
    if let Some(keys) = &ctx.pred.keys
        && let Some(leaf) = leaf_index(&meta, schema::KEY)
    {
        bloom_checks.push((leaf, keys));
    }

    let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta);
    if !bloom_checks.is_empty() {
        for (rg, k) in keep.iter_mut().enumerate() {
            if !*k {
                continue;
            }
            for (leaf, values) in &bloom_checks {
                if let Some(sbbf) = builder
                    .get_row_group_column_bloom_filter(rg, *leaf)
                    .await
                    .map_err(model::Error::from)?
                    && !values.iter().any(|v| sbbf.check(v.as_slice()))
                {
                    *k = false;
                    ctx.metrics
                        .row_groups_pruned_bloom
                        .fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
    }
    let row_groups: Vec<usize> = (0..n).filter(|i| keep[*i]).collect();
    if row_groups.is_empty() {
        return Ok(Vec::new());
    }

    let roots: Vec<usize> = ctx
        .schema
        .fields()
        .iter()
        .filter_map(|f| file_schema.index_of(f.name()).ok())
        .collect();
    builder = builder
        .with_row_groups(row_groups)
        .with_projection(ProjectionMask::roots(&descr, roots))
        .with_batch_size(8192);
    if !pivot_filters.is_empty() {
        let filter_roots: Vec<usize> = pivot_filters
            .iter()
            .filter_map(|(c, _)| file_schema.index_of(c).ok())
            .collect();
        let predicate = ArrowPredicateFn::new(
            ProjectionMask::roots(&descr, filter_roots),
            move |batch: RecordBatch| -> std::result::Result<BooleanArray, ArrowError> {
                let mut mask = vec![true; batch.num_rows()];
                for (col, values) in &pivot_filters {
                    let Some(array) = batch.column_by_name(col) else {
                        continue;
                    };
                    let array = array.as_binary::<i32>();
                    for (i, m) in mask.iter_mut().enumerate() {
                        *m &= !array.is_null(i)
                            && values.iter().any(|v| v.as_slice() == array.value(i));
                    }
                }
                Ok(BooleanArray::from(mask))
            },
        );
        builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
    }
    let batches: Vec<RecordBatch> = builder
        .build()
        .map_err(model::Error::from)?
        .try_collect()
        .await
        .map_err(model::Error::from)?;
    batches.iter().map(|b| project_to(b, &ctx.schema)).collect()
}

#[derive(Debug)]
struct SegmentsStream {
    ctx: Arc<ScanContext>,
    segments: Vec<AddSegment>,
}

impl PartitionStream for SegmentsStream {
    fn schema(&self) -> &SchemaRef {
        &self.ctx.schema
    }

    fn execute(&self, _task: Arc<TaskContext>) -> SendableRecordBatchStream {
        let (tx, rx) = tokio::sync::mpsc::channel::<DfResult<RecordBatch>>(4);
        let ctx = self.ctx.clone();
        let segments = self.segments.clone();
        tokio::spawn(async move {
            let mut results = futures::stream::iter(segments)
                .map(|seg| {
                    let ctx = ctx.clone();
                    async move { scan_segment(&ctx, &seg).await }
                })
                .buffered(4);
            while let Some(r) = results.next().await {
                match r {
                    Ok(batches) => {
                        for b in batches {
                            if b.num_rows() > 0 && tx.send(Ok(b)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
                        return;
                    }
                }
            }
        });
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Box::pin(RecordBatchStreamAdapter::new(
            self.ctx.schema.clone(),
            stream,
        ))
    }
}

/// DataFusion table over one pinned snapshot.
#[derive(Debug)]
pub struct TopicsTable {
    snapshot: Arc<Snapshot>,
    root: Path,
    cache: Arc<FileCache>,
    metrics: Arc<ScanMetrics>,
}

impl TopicsTable {
    pub fn new(
        snapshot: Arc<Snapshot>,
        root: Path,
        cache: Arc<FileCache>,
        metrics: Arc<ScanMetrics>,
    ) -> Self {
        Self {
            snapshot,
            root,
            cache,
            metrics,
        }
    }
}

impl fmt::Debug for FileCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FileCache")
    }
}

#[async_trait]
impl TableProvider for TopicsTable {
    fn schema(&self) -> SchemaRef {
        base_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if Predicate::understands(f) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let pred = Predicate::from_filters(filters);
        let all: Vec<usize> = (0..base_schema().fields().len()).collect();
        let schema = Arc::new(base_schema().project(projection.unwrap_or(&all))?);
        let total = self.snapshot.segments.len();
        let mut segments: Vec<AddSegment> = self
            .snapshot
            .segments
            .values()
            .filter(|s| pred.may_match_segment(s))
            .cloned()
            .collect();
        segments.sort_by_key(|s| std::cmp::Reverse(s.size));
        self.metrics
            .files_total
            .fetch_add(total as u64, Ordering::Relaxed);
        self.metrics
            .files_pruned
            .fetch_add((total - segments.len()) as u64, Ordering::Relaxed);

        let ctx = Arc::new(ScanContext {
            cache: self.cache.clone(),
            root: self.root.clone(),
            pred,
            schema: schema.clone(),
            metrics: self.metrics.clone(),
        });
        let n = state
            .config()
            .target_partitions()
            .clamp(1, segments.len().max(1));
        let mut buckets: Vec<Vec<AddSegment>> = vec![Vec::new(); n];
        for (i, s) in segments.into_iter().enumerate() {
            buckets[i % n].push(s);
        }
        let partitions: Vec<Arc<dyn PartitionStream>> = buckets
            .into_iter()
            .map(|segments| {
                Arc::new(SegmentsStream {
                    ctx: ctx.clone(),
                    segments,
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            partitions,
            None,
            Vec::<LexOrdering>::new(),
            false,
            limit,
        )?))
    }
}
