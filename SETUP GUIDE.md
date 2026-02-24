# **Setup Guide**

Quick setup instructions for the Task Graph Executor.

## **Requirements**

* Python 3.7 or higher  
* No external dependencies required

Check your Python version:

python \--version

## **Installation**

### **Option 1: Clone Repository**

git clone https://github.com/yourusername/task-graph-executor.git

cd task-graph-executor

### **Option 2: Download Files**

Download these files to a directory:

* `task_graph_executor.py`  
* `task_graph_tests.py`  
* `task_graph_visualization.py`

## **Verification**

### **Run Basic Demo**

python task\_graph\_executor.py

Expected output:

\======================================================================

TASK GRAPH EXECUTOR DEMO \- Build System

\======================================================================

Task Dependency Graph:

...

Executing task: compile\_main

Executing task: compile\_utils

...

### **Run Tests**

python task\_graph\_tests.py

Expected output:

test\_task\_creation ... ok

test\_task\_execution\_success ... ok

...

\----------------------------------------------------------------------

Ran 40 tests in 5.234s

OK

### **Run Visualization Demo**

python task\_graph\_visualization.py

Then open `http://localhost:8090` in your browser.

## **Project Structure**

task-graph-executor/

├── task\_graph\_executor.py       \# Core implementation

├── task\_graph\_tests.py          \# Unit tests

├── task\_graph\_visualization.py  \# Visualization features

├── README.md                     \# Overview

├── DOCUMENTATION.md              \# API reference

├── SETUP\_GUIDE.md                \# This file

└── PROJECT\_SUMMARY.md            \# Portfolio document

## **Quick Start Example**

Create a file `example.py`:

from task\_graph\_executor import TaskGraphExecutor, Task

import time

def task\_a():

    time.sleep(1)

    return "Result A"

def task\_b(input\_data):

    time.sleep(1)

    return f"Processed {input\_data}"

\# Create executor

executor \= TaskGraphExecutor(max\_workers=2)

\# Add tasks

executor.add\_task(Task("task\_a", task\_a))

executor.add\_task(Task(

    "task\_b",

    task\_b,

    args=("data",),

    dependencies=\["task\_a"\]

))

\# Execute

results \= executor.execute()

print(results)

Run it:

python example.py

## **Troubleshooting**

### **Issue: Python not found**

Solution:

\# Try python3 instead

python3 task\_graph\_executor.py

### **Issue: ModuleNotFoundError**

Solution:

\# Ensure you're in the correct directory

pwd  \# Should show /path/to/task-graph-executor

\# Verify files exist

ls \-la task\_graph\_executor.py

### **Issue: Port 8090 already in use**

Solution:

\# Change port in code

executor \= VisualizationExecutor(monitoring\_port=8091)

Or find and kill the process:

\# macOS/Linux

lsof \-i :8090

kill \-9 \<PID\>

\# Windows

netstat \-ano | findstr :8090

taskkill /PID \<PID\> /F

### **Issue: Tests fail with timing errors**

This is rare but can happen due to system load. Simply run tests again:

python task\_graph\_tests.py

## **Running Specific Tests**

\# Run all tests

python task\_graph\_tests.py

\# Run specific test class

python \-m unittest task\_graph\_tests.TestGraphValidation

\# Run single test

python \-m unittest task\_graph\_tests.TestGraphValidation.test\_detect\_simple\_cycle

\# Verbose output

python task\_graph\_tests.py \-v

## **Using in Your Project**

### **Import in Python Code**

\# Core functionality

from task\_graph\_executor import TaskGraphExecutor, Task, TaskStatus

\# With visualization

from task\_graph\_visualization import VisualizationExecutor

\# Exceptions

from task\_graph\_executor import CyclicDependencyError

### **Basic Integration**

from task\_graph\_executor import TaskGraphExecutor, Task

def your\_workflow():

    executor \= TaskGraphExecutor(max\_workers=4)

    

    \# Add your tasks

    executor.add\_task(Task("step1", your\_function1))

    executor.add\_task(Task("step2", your\_function2, dependencies=\["step1"\]))

    

    \# Execute

    try:

        results \= executor.execute()

        return results

    except CyclicDependencyError:

        print("Error: Circular dependencies detected")

        return None

## **Performance Tuning**

### **Adjusting Worker Count**

import os

\# For CPU-intensive tasks

workers \= os.cpu\_count()

\# For I/O-intensive tasks

workers \= os.cpu\_count() \* 2

\# Create executor

executor \= TaskGraphExecutor(max\_workers=workers)

### **Optimizing Task Granularity**

Break large tasks into smaller ones for better parallelism:

\# Instead of one large task

executor.add\_task(Task("process\_all\_data", process\_everything))

\# Use multiple smaller tasks

for i in range(10):

    executor.add\_task(Task(f"process\_chunk\_{i}", process\_chunk, args=(i,)))

## **Development Setup**

For development and testing:

\# Create virtual environment (optional)

python \-m venv venv

source venv/bin/activate  \# On Windows: venv\\Scripts\\activate

\# Run tests with coverage (if coverage.py installed)

coverage run task\_graph\_tests.py

coverage report

## **Platform-Specific Notes**

### **Windows**

* Use backslashes in paths: `C:\Users\...\task-graph-executor`  
* Use `python` instead of  `python3`  
* Use Task Manager to kill processes on ports

### **macOS/Linux**

* May need `python3` command  
* Use `lsof` to check ports  
* Consider using `screen` or `tmux` for long-running demos

### **Docker (Optional)**

Create `Dockerfile`:

FROM python:3.9-slim

WORKDIR /app

COPY \*.py /app/

CMD \["python", "task\_graph\_executor.py"\]

Build and run:

docker build \-t task-graph-executor .

docker run task-graph-executor

## **Next Steps**

1. Read `DOCUMENTATION.md` for complete API reference  
2. Review examples in demo functions  
3. Check test cases for usage patterns  
4. Experiment with visualization dashboard

## **Support**

For issues or questions:

* Check `DOCUMENTATION.pdf` for API details  
* Review test cases for examples  
* Examine demo code in main files

## **Verification Checklist**

Before considering setup complete:

* \[ \] Python 3.7+ installed and verified  
* \[ \] All files in same directory  
* \[ \] Basic demo runs without errors  
* \[ \] Tests pass successfully  
* \[ \] Visualization demo accessible at localhost:8090  
* \[ \] Can import modules in Python interpreter  
* \[ \] Understand basic usage from examples

Setup is complete when all demos run successfully and tests pass.

