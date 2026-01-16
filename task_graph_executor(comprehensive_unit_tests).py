"""
Task Graph Executor - Comprehensive Unit Tests

Test Coverage:
1. Basic Task Execution
2. Dependency Resolution
3. Cycle Detection
4. Topological Sorting
5. Parallel Execution
6. Failure Handling & Retries
7. Task Skipping (cascade failures)
8. Edge Cases
"""

import unittest
import time
import threading
from unittest.mock import Mock, patch
from task_graph_executor import (
    Task,
    TaskGraphExecutor,
    TaskStatus,
    CyclicDependencyError
)


class TestTaskBasics(unittest.TestCase):
    """Test basic Task functionality"""
    
    def test_task_creation(self):
        """Task should be created with correct attributes"""
        def dummy_func():
            return "result"
        
        task = Task(
            name="test_task",
            func=dummy_func,
            dependencies=["dep1", "dep2"],
            args=(1, 2),
            kwargs={"key": "value"}
        )
        
        self.assertEqual(task.name, "test_task")
        self.assertEqual(task.func, dummy_func)
        self.assertEqual(task.dependencies, ["dep1", "dep2"])
        self.assertEqual(task.args, (1, 2))
        self.assertEqual(task.kwargs, {"key": "value"})
        self.assertEqual(task.status, TaskStatus.PENDING)
    
    def test_task_execution_success(self):
        """Task should execute successfully and record result"""
        def add(a, b):
            return a + b
        
        task = Task(name="add", func=add, args=(2, 3))
        result = task.execute()
        
        self.assertEqual(result, 5)
        self.assertEqual(task.result, 5)
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertIsNotNone(task.start_time)
        self.assertIsNotNone(task.end_time)
        self.assertGreater(task.duration(), 0)
    
    def test_task_execution_failure(self):
        """Task should capture exceptions on failure"""
        def failing_func():
            raise ValueError("Test error")
        
        task = Task(name="fail", func=failing_func)
        
        with self.assertRaises(ValueError):
            task.execute()
        
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertIsNotNone(task.error)
        self.assertIsInstance(task.error, ValueError)


class TestGraphValidation(unittest.TestCase):
    """Test graph validation and cycle detection"""
    
    def test_simple_valid_graph(self):
        """Valid graph should pass validation"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.add_task(Task("B", lambda: "b", dependencies=["A"]))
        executor.add_task(Task("C", lambda: "c", dependencies=["B"]))
        
        # Should not raise
        executor.validate_graph()
    
    def test_detect_simple_cycle(self):
        """Should detect simple A→B→A cycle"""
        executor = TaskGraphExecutor()
        
        # This creates a cycle, but we can't enforce it in add_task
        # So we manually create the cycle in the dependencies
        executor.add_task(Task("A", lambda: "a"))
        executor.add_task(Task("B", lambda: "b"))
        
        # Manually create cycle
        executor.dependencies["A"].add("B")
        executor.dependencies["B"].add("A")
        executor.dependents["A"].add("B")
        executor.dependents["B"].add("A")
        
        with self.assertRaises(CyclicDependencyError):
            executor.validate_graph()
    
    def test_detect_complex_cycle(self):
        """Should detect cycle in larger graph: A→B→C→D→B"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.add_task(Task("B", lambda: "b"))
        executor.add_task(Task("C", lambda: "c"))
        executor.add_task(Task("D", lambda: "d"))
        
        # Create cycle B→C→D→B
        executor.dependencies["B"].add("A")
        executor.dependencies["C"].add("B")
        executor.dependencies["D"].add("C")
        executor.dependencies["B"].add("D")  # Creates cycle
        
        with self.assertRaises(CyclicDependencyError):
            executor.validate_graph()
    
    def test_missing_dependency(self):
        """Should detect reference to non-existent task"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a", dependencies=["NonExistent"]))
        
        with self.assertRaises(ValueError) as context:
            executor.validate_graph()
        
        self.assertIn("non-existent", str(context.exception).lower())
    
    def test_duplicate_task_name(self):
        """Should prevent adding task with duplicate name"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        
        with self.assertRaises(ValueError) as context:
            executor.add_task(Task("A", lambda: "a2"))
        
        self.assertIn("already exists", str(context.exception))


class TestTopologicalSort(unittest.TestCase):
    """Test topological sorting algorithm"""
    
    def test_simple_chain(self):
        """Should sort simple chain A→B→C"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.add_task(Task("B", lambda: "b", dependencies=["A"]))
        executor.add_task(Task("C", lambda: "c", dependencies=["B"]))
        
        order = executor.topological_sort()
        
        # A must come before B, B before C
        self.assertEqual(order.index("A"), 0)
        self.assertEqual(order.index("B"), 1)
        self.assertEqual(order.index("C"), 2)
    
    def test_diamond_graph(self):
        """Should sort diamond: A→B, A→C, B→D, C→D"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.add_task(Task("B", lambda: "b", dependencies=["A"]))
        executor.add_task(Task("C", lambda: "c", dependencies=["A"]))
        executor.add_task(Task("D", lambda: "d", dependencies=["B", "C"]))
        
        order = executor.topological_sort()
        
        # A must be first
        self.assertEqual(order[0], "A")
        # D must be last
        self.assertEqual(order[3], "D")
        # B and C can be in any order, but both after A and before D
        self.assertIn("B", order[1:3])
        self.assertIn("C", order[1:3])
    
    def test_multiple_roots(self):
        """Should handle graph with multiple starting nodes"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.add_task(Task("B", lambda: "b"))
        executor.add_task(Task("C", lambda: "c", dependencies=["A", "B"]))
        
        order = executor.topological_sort()
        
        # A and B have no dependencies (can be in any order)
        self.assertIn("A", order[:2])
        self.assertIn("B", order[:2])
        # C depends on both, must be last
        self.assertEqual(order[2], "C")


class TestDependencyResolution(unittest.TestCase):
    """Test runtime dependency resolution"""
    
    def test_get_ready_tasks_no_dependencies(self):
        """Tasks with no dependencies should be immediately ready"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.add_task(Task("B", lambda: "b"))
        
        ready = executor.get_ready_tasks()
        
        self.assertEqual(set(ready), {"A", "B"})
    
    def test_get_ready_tasks_with_dependencies(self):
        """Tasks should become ready only after dependencies complete"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.add_task(Task("B", lambda: "b", dependencies=["A"]))
        
        # Initially, only A is ready
        ready = executor.get_ready_tasks()
        self.assertEqual(ready, ["A"])
        
        # After A completes, B becomes ready
        executor.completed_tasks.add("A")
        ready = executor.get_ready_tasks()
        self.assertEqual(ready, ["B"])
    
    def test_ready_tasks_skip_running(self):
        """Should not return tasks already running"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.running_tasks.add("A")
        
        ready = executor.get_ready_tasks()
        
        self.assertEqual(ready, [])
    
    def test_ready_tasks_skip_completed(self):
        """Should not return tasks already completed"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.completed_tasks.add("A")
        
        ready = executor.get_ready_tasks()
        
        self.assertEqual(ready, [])


class TestParallelExecution(unittest.TestCase):
    """Test parallel execution of independent tasks"""
    
    def test_independent_tasks_run_parallel(self):
        """Independent tasks should run simultaneously"""
        executor = TaskGraphExecutor(max_workers=3)
        
        execution_times = {}
        lock = threading.Lock()
        
        def timed_task(name, duration):
            start = time.time()
            time.sleep(duration)
            with lock:
                execution_times[name] = (start, time.time())
            return name
        
        # Add 3 independent tasks that take 0.5s each
        for i in range(3):
            executor.add_task(Task(
                f"task_{i}",
                timed_task,
                args=(f"task_{i}", 0.5)
            ))
        
        start_time = time.time()
        executor.execute()
        total_time = time.time() - start_time
        
        # If parallel, should take ~0.5s, not 1.5s
        self.assertLess(total_time, 1.0)
        
        # Check that tasks overlapped in time
        times = list(execution_times.values())
        # At least two tasks should have overlapping execution
        overlaps = 0
        for i, (s1, e1) in enumerate(times):
            for s2, e2 in times[i+1:]:
                if s1 < e2 and s2 < e1:  # Overlap condition
                    overlaps += 1
        self.assertGreater(overlaps, 0)
    
    def test_respects_max_workers(self):
        """Should not exceed max_workers concurrent tasks"""
        executor = TaskGraphExecutor(max_workers=2)
        
        concurrent_count = [0]
        max_concurrent = [0]
        lock = threading.Lock()
        
        def concurrent_task():
            with lock:
                concurrent_count[0] += 1
                max_concurrent[0] = max(max_concurrent[0], concurrent_count[0])
            
            time.sleep(0.2)
            
            with lock:
                concurrent_count[0] -= 1
        
        # Add 5 independent tasks
        for i in range(5):
            executor.add_task(Task(f"task_{i}", concurrent_task))
        
        executor.execute()
        
        # Should never exceed max_workers
        self.assertLessEqual(max_concurrent[0], 2)


class TestTaskExecution(unittest.TestCase):
    """Test end-to-end task execution"""
    
    def test_simple_sequential_execution(self):
        """Should execute simple chain correctly"""
        executor = TaskGraphExecutor()
        
        results = []
        
        def task_a():
            results.append("A")
            return "A"
        
        def task_b():
            results.append("B")
            return "B"
        
        def task_c():
            results.append("C")
            return "C"
        
        executor.add_task(Task("A", task_a))
        executor.add_task(Task("B", task_b, dependencies=["A"]))
        executor.add_task(Task("C", task_c, dependencies=["B"]))
        
        executor.execute()
        
        # Should execute in order: A, B, C
        self.assertEqual(results, ["A", "B", "C"])
    
    def test_diamond_execution(self):
        """Should handle diamond dependency correctly"""
        executor = TaskGraphExecutor(max_workers=2)
        
        def task_func(name):
            time.sleep(0.1)
            return name
        
        executor.add_task(Task("A", task_func, args=("A",)))
        executor.add_task(Task("B", task_func, args=("B",), dependencies=["A"]))
        executor.add_task(Task("C", task_func, args=("C",), dependencies=["A"]))
        executor.add_task(Task("D", task_func, args=("D",), dependencies=["B", "C"]))
        
        results = executor.execute()
        
        # All tasks should complete
        self.assertEqual(len(results), 4)
        self.assertEqual(set(results.keys()), {"A", "B", "C", "D"})
    
    def test_execution_with_arguments(self):
        """Tasks should receive correct arguments"""
        executor = TaskGraphExecutor()
        
        def add(a, b):
            return a + b
        
        def multiply(x, factor=1):
            return x * factor
        
        executor.add_task(Task("add", add, args=(2, 3)))
        executor.add_task(Task("multiply", multiply, args=(10,), kwargs={"factor": 5}))
        
        results = executor.execute()
        
        self.assertEqual(results["add"], 5)
        self.assertEqual(results["multiply"], 50)


class TestFailureHandling(unittest.TestCase):
    """Test task failure and retry logic"""
    
    def test_task_failure_recorded(self):
        """Failed tasks should be marked as FAILED"""
        executor = TaskGraphExecutor()
        
        def failing_task():
            raise ValueError("Expected failure")
        
        executor.add_task(Task("fail", failing_task))
        
        executor.execute()
        
        self.assertIn("fail", executor.failed_tasks)
        self.assertEqual(executor.tasks["fail"].status, TaskStatus.FAILED)
        self.assertIsNotNone(executor.tasks["fail"].error)
    
    def test_retry_on_failure(self):
        """Should retry failed tasks"""
        executor = TaskGraphExecutor()
        
        attempt_count = [0]
        
        def flaky_task():
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise ValueError("Transient error")
            return "success"
        
        executor.add_task(Task("flaky", flaky_task, retries=3))
        
        results = executor.execute()
        
        # Should succeed after retries
        self.assertEqual(results["flaky"], "success")
        self.assertEqual(attempt_count[0], 3)
        self.assertEqual(executor.tasks["flaky"].retry_count, 2)
    
    def test_retry_exhaustion(self):
        """Should fail after exhausting retries"""
        executor = TaskGraphExecutor()
        
        def always_fail():
            raise ValueError("Always fails")
        
        executor.add_task(Task("fail", always_fail, retries=2))
        
        executor.execute()
        
        self.assertIn("fail", executor.failed_tasks)
        self.assertEqual(executor.tasks["fail"].retry_count, 2)
    
    def test_dependent_task_skipped_on_failure(self):
        """Dependent tasks should be skipped if dependency fails"""
        executor = TaskGraphExecutor()
        
        def failing_task():
            raise ValueError("Fail")
        
        def dependent_task():
            return "should not execute"
        
        executor.add_task(Task("fail", failing_task))
        executor.add_task(Task("dependent", dependent_task, dependencies=["fail"]))
        
        results = executor.execute()
        
        # Dependent should be skipped
        self.assertIn("fail", executor.failed_tasks)
        self.assertIn("dependent", executor.failed_tasks)
        self.assertEqual(executor.tasks["dependent"].status, TaskStatus.SKIPPED)
        self.assertNotIn("dependent", results)
    
    def test_cascade_failure_skipping(self):
        """Cascade failures should skip entire dependency chain"""
        executor = TaskGraphExecutor()
        
        def fail():
            raise ValueError("Root failure")
        
        def should_skip():
            return "should not run"
        
        executor.add_task(Task("A", fail))
        executor.add_task(Task("B", should_skip, dependencies=["A"]))
        executor.add_task(Task("C", should_skip, dependencies=["B"]))
        
        executor.execute()
        
        # All should be marked as failed/skipped
        self.assertIn("A", executor.failed_tasks)
        self.assertIn("B", executor.failed_tasks)
        self.assertIn("C", executor.failed_tasks)
        
        self.assertEqual(executor.tasks["B"].status, TaskStatus.SKIPPED)
        self.assertEqual(executor.tasks["C"].status, TaskStatus.SKIPPED)


class TestExecutionStatistics(unittest.TestCase):
    """Test execution statistics and monitoring"""
    
    def test_execution_stats_structure(self):
        """Stats should have correct structure"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.execute()
        
        stats = executor.get_execution_stats()
        
        self.assertIn("total_tasks", stats)
        self.assertIn("completed", stats)
        self.assertIn("failed", stats)
        self.assertIn("running", stats)
        self.assertIn("task_details", stats)
    
    def test_stats_accuracy(self):
        """Stats should accurately reflect execution"""
        executor = TaskGraphExecutor()
        
        def success():
            time.sleep(0.1)
            return "ok"
        
        def fail():
            raise ValueError("Fail")
        
        executor.add_task(Task("success", success))
        executor.add_task(Task("fail", fail))
        
        executor.execute()
        stats = executor.get_execution_stats()
        
        self.assertEqual(stats["total_tasks"], 2)
        self.assertEqual(stats["completed"], 1)
        self.assertEqual(stats["failed"], 1)
        
        # Check task details
        self.assertEqual(stats["task_details"]["success"]["status"], "COMPLETED")
        self.assertGreater(stats["task_details"]["success"]["duration"], 0)
        self.assertEqual(stats["task_details"]["fail"]["status"], "FAILED")
        self.assertIsNotNone(stats["task_details"]["fail"]["error"])


class TestVisualization(unittest.TestCase):
    """Test graph visualization"""
    
    def test_visualization_output(self):
        """Should generate visualization string"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("A", lambda: "a"))
        executor.add_task(Task("B", lambda: "b", dependencies=["A"]))
        
        viz = executor.visualize_graph()
        
        self.assertIsInstance(viz, str)
        self.assertIn("A", viz)
        self.assertIn("B", viz)
        self.assertIn("Dependency Graph", viz)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions"""
    
    def test_empty_graph(self):
        """Should handle empty graph"""
        executor = TaskGraphExecutor()
        
        results = executor.execute()
        
        self.assertEqual(results, {})
    
    def test_single_task(self):
        """Should handle single task"""
        executor = TaskGraphExecutor()
        
        executor.add_task(Task("solo", lambda: "alone"))
        
        results = executor.execute()
        
        self.assertEqual(results["solo"], "alone")
    
    def test_many_independent_tasks(self):
        """Should handle many independent tasks"""
        executor = TaskGraphExecutor(max_workers=4)
        
        for i in range(20):
            executor.add_task(Task(f"task_{i}", lambda x=i: x))
        
        results = executor.execute()
        
        self.assertEqual(len(results), 20)
    
    def test_deep_dependency_chain(self):
        """Should handle deep dependency chains"""
        executor = TaskGraphExecutor()
        
        # Create chain of 50 tasks
        executor.add_task(Task("task_0", lambda: 0))
        for i in range(1, 50):
            executor.add_task(Task(
                f"task_{i}",
                lambda x=i: x,
                dependencies=[f"task_{i-1}"]
            ))
        
        results = executor.execute()
        
        self.assertEqual(len(results), 50)


def run_tests():
    """Run all tests with verbose output"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestTaskBasics,
        TestGraphValidation,
        TestTopologicalSort,
        TestDependencyResolution,
        TestParallelExecution,
        TestTaskExecution,
        TestFailureHandling,
        TestExecutionStatistics,
        TestVisualization,
        TestEdgeCases
    ]
    
    for test_class in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(test_class))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print("="*70)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)