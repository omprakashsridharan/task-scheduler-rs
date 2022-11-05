use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use warp::reject;

#[derive(BorshDeserialize, BorshSerialize, Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct Task {
    pub task_id: String,
    pub task_type: String,
    pub time_to_live_in_seconds: usize,
    pub payload: String,
}

impl Task {
    pub fn new(task_type: String, time_to_live_in_seconds: usize, payload: String) -> Self {
        Self {
            task_id: Uuid::new_v4().to_string(),
            task_type,
            time_to_live_in_seconds,
            payload,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct TaskRequest {
    pub task_type: String,
    pub time_to_live_in_seconds: usize,
    pub payload: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ConsumeRequest {
    pub task_type: String,
}

#[derive(Debug)]
pub struct TaskExpired;

impl reject::Reject for TaskExpired {}
