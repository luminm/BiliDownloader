use anyhow::Result;
use sea_query::{
    ColumnDef, Iden, OnConflict, Query, SqliteQueryBuilder, Table, TableCreateStatement,
};
use sea_query_binder::SqlxBinder;
use sqlx::Row;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::queue::{atomics::QueueType, manager::MANAGER};

use super::db::{get_db, TableSpec};

#[derive(Iden, Clone, Copy)]
pub enum Queue {
    Table,
    Name,
    Value,
}

pub struct QueueTable;

impl TableSpec for QueueTable {
    const NAME: &'static str = "queue";
    const LATEST: i32 = 1;
    fn create_stmt() -> TableCreateStatement {
        Table::create()
            .table(Queue::Table)
            .col(
                ColumnDef::new(Queue::Name)
                    .integer()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Queue::Value).text().not_null())
            .to_owned()
    }
}

// Loads the queue lists and drops the entries that lost their task or
// scheduler. A stale id renders as an empty card, a scheduler left in two
// lists renders twice, so the stored lists are repaired on load.
pub async fn load() -> Result<HashMap<QueueType, Vec<String>>> {
    let (sql, values) = Query::select()
        .columns([Queue::Table, Queue::Name, Queue::Value])
        .from(Queue::Table)
        .build_sqlx(SqliteQueryBuilder);

    let pool = get_db().await?;
    let rows = sqlx::query_with(&sql, values).fetch_all(&pool).await?;

    let mut map = HashMap::new();

    for r in rows {
        let q = r.try_get::<u8, _>("name")?;
        let v: Vec<String> = serde_json::from_str(&r.try_get::<String, _>("value")?)?;
        let key: QueueType = q.into();
        let mut queue = MANAGER.get_queue(&key).write().await;

        queue.clear();
        queue.extend(v.clone());

        drop(queue);

        map.insert(key, v);
    }

    let tasks = MANAGER.tasks.read().await;
    let schedulers = MANAGER.schedulers.read().await;

    let mut seen = HashSet::new();
    let mut result: HashMap<QueueType, Vec<String>> = HashMap::new();

    for key in [
        QueueType::Backlog,
        QueueType::Pending,
        QueueType::Doing,
        QueueType::Complete,
    ] {
        let mut queue = MANAGER.get_queue(&key).write().await;
        let before = queue.len();
        queue.retain(|id| {
            let known = match key {
                QueueType::Backlog => tasks.contains_key(id),
                _ => schedulers.get(id).is_some_and(|v| v.queue.get() == key),
            };
            known && seen.insert(id.clone())
        });
        let changed = queue.len() != before;
        let value: VecDeque<String> = queue.iter().cloned().collect();
        drop(queue);
        if changed {
            upsert(key, &value).await?;
        }
        result.insert(key, value.into_iter().collect());
    }

    // A scheduler that lost its entry belongs to the list it reports itself
    for (sid, scheduler) in schedulers.iter() {
        let key = scheduler.queue.get();
        if key == QueueType::Backlog || seen.contains(sid) {
            continue;
        }
        let mut queue = MANAGER.get_queue(&key).write().await;
        queue.push_back(sid.clone());
        let value: VecDeque<String> = queue.iter().cloned().collect();
        drop(queue);
        upsert(key, &value).await?;
        if let Some(value) = result.get_mut(&key) {
            value.push(sid.clone());
        }
    }

    Ok(result)
}

pub async fn upsert(name: QueueType, value: &VecDeque<String>) -> Result<()> {
    let pool = get_db().await?;
    let val = serde_json::to_string(value)?;
    let (sql, values) = Query::insert()
        .into_table(Queue::Table)
        .columns([Queue::Name, Queue::Value])
        .values([(name as u8).into(), val.into()])?
        .on_conflict(
            OnConflict::column(Queue::Name)
                .update_columns([Queue::Value])
                .to_owned(),
        )
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&sql, values).execute(&pool).await?;
    Ok(())
}
