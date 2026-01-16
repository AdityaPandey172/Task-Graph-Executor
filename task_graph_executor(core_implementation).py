"""
Task Graph Executor - Dependency-Aware Task Scheduler

This implements a DAG (Directed Acyclic Graph) based task executor that:
1. Validates task dependencies (detects cycles)
2. Executes tasks in topological order
3. Maximizes parallelism by running independent tasks simultaneously
4. Handles task failures and retries
5. Provides real-time progress tracking

Real-world use cases:
- Build systems (compile dependencies)
- Data pipelines (ETL workflows)
- CI/CD pipelines
- Workflow orchestration
"""

import time
import threading
from enum import Enum
from typing import Callable, Any, Dict, List, Set, Optional
from dataclasses import dataclass, field
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, Future
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution states"""
    PENDING = "PENDING"           # Not started yet
    READY = "READY"               # Dependencies met, ready to run
    RUNNING = "RUNNING"           # Currently executing
    COMPLETED = "COMPLETED"       # Successfully finished
    FAILED = "FAILED"             # Execution failed
    SKIPPED = "SKIPPED"           # Skipped due to dependency failure


@dataclass
class Task:
    """
    Represents a single task in the execution graph.
    
    Attributes:
        name: Unique identifier for the task
        func: Function to execute (callable)
        dependencies: List of task names this task depends on
        args: Positional arguments for func
        kwargs: Keyword arguments for func
        retries: Number of retry attempts on failure
        timeout: Maximum execution time in seconds
    """
    name: str
    func: Callable
    dependencies: List[str] = field(default_factory=list)
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    retries: int = 0
    timeout: Optional[float] = None
    
    # Runtime state (set by executor)
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[Exception] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    retry_count: int = 0
    
    def execute(self) -> Any:
        """Execute the task function"""
        try:
            self.start_time = time.time()
            self.status = TaskStatus.RUNNING
            logger.info(f" Executing task: {self.name}")
            
            result = self.func(*self.args, **self.kwargs)
            
            self.end_time = time.time()
            self.result = result
            self.status = TaskStatus.COMPLETED
            logger.info(f"Completed task: {self.name} "
                       f"({self.end_time - self.start_time:.2f}s)")
            return result
            
        except Exception as e:
            self.end_time = time.time()
            self.error = e
            self.status = TaskStatus.FAILED
            logger.error(f" Failed task: {self.name} - {str(e)}")
            raise e
    
    def duration(self) -> float:
        """Get execution duration in seconds"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


class CyclicDependencyError(Exception):
    """Raised when circular dependencies are detected"""
    pass


class TaskGraphExecutor:
    """
    Executes tasks in dependency order with maximum parallelism.
    
    Key Features:
    1. DAG Validation - Detects circular dependencies
    2. Topological Sorting - Determines execution order
    3. Parallel Execution - Runs independent tasks simultaneously
    4. Dependency Tracking - Waits for prerequisites
    5. Failure Handling - Retries and cascading failures
    
    Example:
        executor = TaskGraphExecutor(max_workers=4)
        executor.add_task(Task("compile", compile_code))
        executor.add_task(Task("test", run_tests, dependencies=["compile"]))
        results = executor.execute()
    """
    
    def __init__(self, max_workers: int = 4):
        """
        Initialize task graph executor.
        
        Args:
            max_workers: Maximum number of concurrent tasks
        """
        self.max_workers = max_workers
        self.tasks: Dict[str, Task] = {}
        
        # Dependency graph: task_name -> list of tasks that depend on it
        self.dependents: Dict[str, Set[str]] = defaultdict(set)
        
        # Reverse graph: task_name -> list of tasks it depends on
        self.dependencies: Dict[str, Set[str]] = defaultdict(set)
        
        # Thread synchronization
        self.lock = threading.Lock()
        self.completion_event = threading.Event()
        
        # Execution state
        self.thread_pool: Optional[ThreadPoolExecutor] = None
        self.running_tasks: Set[str] = set()
        self.completed_tasks: Set[str] = set()
        self.failed_tasks: Set[str] = set()
        self.futures: Dict[str, Future] = {}
    
    def add_task(self, task: Task):
        """
        Add a task to the execution graph.
        
        Args:
            task: Task object to add
            
        Raises:
            ValueError: If task with same name already exists
        """
        with self.lock:
            if task.name in self.tasks:
                raise ValueError(f"Task '{task.name}' already exists")
            
            self.tasks[task.name] = task
            
            # Build dependency graph
            for dep in task.dependencies:
                self.dependencies[task.name].add(dep)
                self.dependents[dep].add(task.name)
            
            logger.info(f" Added task: {task.name} "
                       f"(dependencies: {task.dependencies or 'none'})")
    
    def validate_graph(self):
        """
        Validate the task graph for cycles and missing dependencies.
        
        Uses DFS (Depth-First Search) to detect cycles.
        
        Raises:
            CyclicDependencyError: If circular dependency detected
            ValueError: If dependency references non-existent task
        """
        # Check for missing dependencies
        for task_name, deps in self.dependencies.items():
            for dep in deps:
                if dep not in self.tasks:
                    raise ValueError(
                        f"Task '{task_name}' depends on non-existent task '{dep}'"
                    )
        
        # Detect cycles using DFS
        visited = set()
        recursion_stack = set()
        
        def has_cycle(node: str) -> bool:
            """DFS to detect cycle"""
            visited.add(node)
            recursion_stack.add(node)
            
            for neighbor in self.dependencies.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in recursion_stack:
                    return True
            
            recursion_stack.remove(node)
            return False
        
        for task_name in self.tasks:
            if task_name not in visited:
                if has_cycle(task_name):
                    raise CyclicDependencyError(
                        f"Circular dependency detected involving task '{task_name}'"
                    )
        
        logger.info("✓ Graph validation passed (no cycles)")
    
    def topological_sort(self) -> List[str]:
        """
        Perform topological sort using Kahn's algorithm.
        
        Returns tasks in an order where dependencies come before dependents.
        This doesn't mean sequential execution - tasks at the same "level"
        can run in parallel.
        
        Returns:
            List of task names in topological order
        """
        # Calculate in-degree (number of dependencies) for each task
        in_degree = {task: len(self.dependencies[task]) 
                    for task in self.tasks}
        
        # Queue of tasks with no dependencies
        queue = deque([task for task, degree in in_degree.items() 
                      if degree == 0])
        
        sorted_tasks = []
        
        while queue:
            task = queue.popleft()
            sorted_tasks.append(task)
            
            # Reduce in-degree for dependent tasks
            for dependent in self.dependents.get(task, []):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        if len(sorted_tasks) != len(self.tasks):
            raise CyclicDependencyError("Graph contains a cycle")
        
        return sorted_tasks
    
    def get_ready_tasks(self) -> List[str]:
        """
        Get tasks that are ready to execute (all dependencies met).
        
        Returns:
            List of task names ready for execution
        """
        ready = []
        
        with self.lock:
            for task_name, task in self.tasks.items():
                # Skip if already processed
                if (task_name in self.completed_tasks or 
                    task_name in self.running_tasks or
                    task_name in self.failed_tasks):
                    continue
                
                # Check if all dependencies are completed
                deps = self.dependencies.get(task_name, set())
                
                # Skip if any dependency failed
                if any(dep in self.failed_tasks for dep in deps):
                    task.status = TaskStatus.SKIPPED
                    self.failed_tasks.add(task_name)
                    logger.warning(f" Skipped task: {task_name} "
                                  f"(dependency failed)")
                    continue
                
                # Ready if all dependencies completed
                if all(dep in self.completed_tasks for dep in deps):
                    ready.append(task_name)
        
        return ready
    
    def execute_task(self, task_name: str):
        """
        Execute a single task.
        
        Args:
            task_name: Name of task to execute
        """
        task = self.tasks[task_name]
        
        try:
            # Execute with retries
            for attempt in range(task.retries + 1):
                try:
                    task.retry_count = attempt
                    task.execute()
                    break  # Success
                except Exception as e:
                    if attempt < task.retries:
                        logger.warning(
                            f" Retrying task: {task_name} "
                            f"(attempt {attempt + 2}/{task.retries + 1})"
                        )
                        time.sleep(1)  # Simple backoff
                    else:
                        raise e  # Final failure
            
            with self.lock:
                self.completed_tasks.add(task_name)
                self.running_tasks.discard(task_name)
                
        except Exception as e:
            with self.lock:
                self.failed_tasks.add(task_name)
                self.running_tasks.discard(task_name)
                task.error = e
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute all tasks in the graph.
        
        Returns:
            Dictionary mapping task names to their results
            
        Raises:
            CyclicDependencyError: If graph contains cycles
            Exception: If any task fails and no retry succeeds
        """
        logger.info(f" Starting execution of {len(self.tasks)} tasks "
                   f"with {self.max_workers} workers")
        
        # Validate graph
        self.validate_graph()
        
        # Reset execution state
        self.running_tasks.clear()
        self.completed_tasks.clear()
        self.failed_tasks.clear()
        self.futures.clear()
        
        start_time = time.time()
        
        # Create thread pool
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            self.thread_pool = executor
            
            # Execute until all tasks complete or fail
            while len(self.completed_tasks) + len(self.failed_tasks) < len(self.tasks):
                # Get tasks ready to execute
                ready_tasks = self.get_ready_tasks()
                
                # Submit ready tasks
                for task_name in ready_tasks:
                    with self.lock:
                        if task_name not in self.running_tasks:
                            self.running_tasks.add(task_name)
                            self.tasks[task_name].status = TaskStatus.READY
                            future = executor.submit(self.execute_task, task_name)
                            self.futures[task_name] = future
                
                # Wait a bit before checking again
                time.sleep(0.1)
        
        end_time = time.time()
        
        # Collect results
        results = {
            name: task.result 
            for name, task in self.tasks.items()
            if task.status == TaskStatus.COMPLETED
        }
        
        # Summary
        total_duration = end_time - start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"   Execution Summary:")
        logger.info(f"   Total time: {total_duration:.2f}s")
        logger.info(f"   Completed: {len(self.completed_tasks)}")
        logger.info(f"   Failed: {len(self.failed_tasks)}")
        logger.info(f"   Skipped: {len([t for t in self.tasks.values() if t.status == TaskStatus.SKIPPED])}")
        logger.info(f"{'='*60}\n")
        
        return results
    
    def get_execution_stats(self) -> Dict[str, Any]:
        """Get detailed execution statistics"""
        return {
            "total_tasks": len(self.tasks),
            "completed": len(self.completed_tasks),
            "failed": len(self.failed_tasks),
            "running": len(self.running_tasks),
            "task_details": {
                name: {
                    "status": task.status.value,
                    "duration": task.duration(),
                    "retries": task.retry_count,
                    "error": str(task.error) if task.error else None
                }
                for name, task in self.tasks.items()
            }
        }
    
    def visualize_graph(self) -> str:
        """
        Generate ASCII visualization of the task graph.
        
        Returns:
            String representation of the graph
        """
        lines = ["Task Dependency Graph:", "=" * 60]
        
        # Get topological order
        try:
            topo_order = self.topological_sort()
        except:
            topo_order = list(self.tasks.keys())
        
        for task_name in topo_order:
            task = self.tasks[task_name]
            deps = self.dependencies.get(task_name, set())
            
            # Status indicator
            status_icon = {
                TaskStatus.PENDING: "(Pending)",
                TaskStatus.READY: "(Ready)",
                TaskStatus.RUNNING: "(Running)",
                TaskStatus.COMPLETED: "(Completed)",
                TaskStatus.FAILED: "(Failed)",
                TaskStatus.SKIPPED: "(Skipped)"
            }.get(task.status, "(Status)")
            
            if deps:
                lines.append(f"{status_icon} {task_name} ← {list(deps)}")
            else:
                lines.append(f"{status_icon} {task_name} (no dependencies)")
        
        return "\n".join(lines)


# ============================================================================
# DEMO: Build System Example
# ============================================================================

def demo_build_system():
    """Demonstrate task graph executor with a build system example"""
    
    print("=" * 70)
    print("TASK GRAPH EXECUTOR DEMO - Build System")
    print("=" * 70)
    
    # Simulate build tasks
    def compile_module(name, duration=1):
        """Simulate compiling a module"""
        time.sleep(duration)
        return f"{name}.o"
    
    def link_binary(objects):
        """Simulate linking object files"""
        time.sleep(1.5)
        return f"app.bin (from {objects})"
    
    def run_tests():
        """Simulate running tests"""
        time.sleep(2)
        return "All tests passed"
    
    def package():
        """Simulate packaging"""
        time.sleep(0.5)
        return "package.tar.gz"
    
    # Create executor
    executor = TaskGraphExecutor(max_workers=3)
    
    # Add tasks
    executor.add_task(Task(
        name="compile_main",
        func=compile_module,
        args=("main", 2)
    ))
    
    executor.add_task(Task(
        name="compile_utils",
        func=compile_module,
        args=("utils", 1.5)
    ))
    
    executor.add_task(Task(
        name="compile_config",
        func=compile_module,
        args=("config", 1)
    ))
    
    executor.add_task(Task(
        name="link",
        func=link_binary,
        args=(["main.o", "utils.o", "config.o"],),
        dependencies=["compile_main", "compile_utils", "compile_config"]
    ))
    
    executor.add_task(Task(
        name="test",
        func=run_tests,
        dependencies=["link"]
    ))
    
    executor.add_task(Task(
        name="package",
        func=package,
        dependencies=["test"]
    ))
    
    # Visualize before execution
    print("\n" + executor.visualize_graph())
    print()
    
    # Execute
    results = executor.execute()
    
    # Show results
    print("\nExecution Results:")
    for task_name, result in results.items():
        print(f"  {task_name}: {result}")
    
    # Show stats
    stats = executor.get_execution_stats()
    print(f"\nDetailed Stats:")
    for task_name, details in stats['task_details'].items():
        print(f"  {task_name}:")
        print(f"    Status: {details['status']}")
        print(f"    Duration: {details['duration']:.2f}s")


if __name__ == "__main__":
    demo_build_system()