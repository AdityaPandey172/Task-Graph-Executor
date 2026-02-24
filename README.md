# Task Graph Executor

A production-ready DAG (Directed Acyclic Graph) based task scheduler that executes tasks in dependency order while maximizing parallelism.

## Overview

This implementation demonstrates advanced concurrency patterns, graph algorithms, and distributed system concepts through a practical task execution engine.

### Key Features

- **Dependency Management**: Executes tasks in correct order based on dependencies
- **Parallel Execution**: Runs independent tasks simultaneously using thread pools
- **Cycle Detection**: Validates graphs using Depth-First Search (DFS)
- **Topological Sorting**: Uses Kahn's algorithm for optimal execution order
- **Failure Handling**: Configurable retries and cascade failure management
- **Real-Time Monitoring**: HTTP dashboard with Gantt charts and live metrics
- **Thread-Safe**: Handles concurrent access with proper locking mechanisms

## Quick Start

```python
from task_graph_executor import TaskGraphExecutor, Task

# Create executor
executor = TaskGraphExecutor(max_workers=4)

# Define tasks with dependencies
executor.add_task(Task("compile", compile_code))
executor.add_task(Task("test", run_tests, dependencies=["compile"]))
executor.add_task(Task("package", create_package, dependencies=["test"]))

# Execute
results = executor.execute()
```

## Use Cases

- **Build Systems**: Compile dependencies (Make, Bazel)
- **Data Pipelines**: ETL workflows (Airflow, Luigi)
- **CI/CD**: Pipeline orchestration
- **Workflow Automation**: Complex task orchestration

## Architecture

```
Task Graph: A -> B -> D
            A -> C -> D

Execution Timeline:
Time 0s:  [A starts]
Time 2s:  [A completes, B and C start in parallel]
Time 5s:  [B and C complete, D starts]
Time 6s:  [D completes]

Total: 6 seconds (vs 8 seconds sequential)
```

## Performance

- **Throughput**: Processes complex graphs with 100+ tasks
- **Efficiency**: Achieves near-optimal parallelism through topological sorting
- **Overhead**: Minimal scheduling overhead (<1ms per task)
- **Scalability**: Configurable thread pool for different workloads

## Technical Highlights

### Algorithms Implemented

| Algorithm | Purpose | Complexity |
|-----------|---------|------------|
| DFS Cycle Detection | Find circular dependencies | O(V + E) |
| Kahn's Topological Sort | Determine execution order | O(V + E) |
| Dynamic Scheduling | Maximize parallelism | O(V) per iteration |

### Design Patterns

- State Machine for task lifecycle
- Observer pattern for progress tracking
- Thread Pool pattern for concurrency
- Factory pattern for task creation

## Documentation

- `README.md` - This file
- `DOCUMENTATION.md` - Complete API reference
- `SETUP_GUIDE.md` - Installation and setup
- `PROJECT_SUMMARY.md` - Portfolio document with metrics

## Testing

```bash
python task_graph_tests.py
```

40+ comprehensive tests covering:
- Graph validation and cycle detection
- Topological sorting correctness
- Parallel execution behavior
- Failure handling and retries
- Edge cases and boundary conditions

## Visualization

Run with real-time monitoring:

```python
from task_graph_visualization import VisualizationExecutor

executor = VisualizationExecutor(max_workers=4, monitoring_port=8090)
# Add tasks...
executor.start_monitoring()
executor.execute()
# Open http://localhost:8090 to view dashboard
```

## Requirements

- Python 3.7+
- No external dependencies for core functionality
- Standard library only (threading, collections, concurrent.futures)


## Notes

Built as a demonstration of advanced software engineering concepts including graph algorithms, concurrent programming, and system design.
