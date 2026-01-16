"""
Task Graph Executor - Visualization & Real-Time Monitoring

Features:
1. Real-time execution viewer (terminal UI)
2. Interactive HTML dashboard
3. Gantt chart timeline
4. Execution graph visualization
5. Progress tracking
"""

import time
import json
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from task_graph_executor import TaskGraphExecutor, Task, TaskStatus


class VisualizationExecutor(TaskGraphExecutor):
    """
    Enhanced executor with real-time visualization capabilities.
    
    Extends base executor to track detailed execution timeline and
    provide live monitoring through HTTP dashboard.
    """
    
    def __init__(self, max_workers: int = 4, monitoring_port: int = 8090):
        super().__init__(max_workers)
        self.monitoring_port = monitoring_port
        self.http_server = None
        self.server_thread = None
        
        # Execution timeline for visualization
        self.timeline = []
        self.execution_start_time = None
    
    def execute_task(self, task_name: str):
        """Override to track timeline events"""
        # Record start event
        self._record_event(task_name, "start")
        
        # Execute normally
        super().execute_task(task_name)
        
        # Record end event
        task = self.tasks[task_name]
        event_type = "complete" if task.status == TaskStatus.COMPLETED else "failed"
        self._record_event(task_name, event_type)
    
    def _record_event(self, task_name: str, event_type: str):
        """Record timeline event for visualization"""
        if self.execution_start_time is None:
            self.execution_start_time = time.time()
        
        relative_time = time.time() - self.execution_start_time
        
        self.timeline.append({
            "task": task_name,
            "event": event_type,
            "timestamp": time.time(),
            "relative_time": relative_time
        })
    
    def get_gantt_data(self):
        """
        Generate Gantt chart data for timeline visualization.
        
        Returns:
            List of task execution intervals
        """
        gantt_data = []
        
        for task_name, task in self.tasks.items():
            if task.start_time and task.end_time:
                gantt_data.append({
                    "task": task_name,
                    "start": task.start_time - (self.execution_start_time or 0),
                    "duration": task.duration(),
                    "status": task.status.value
                })
        
        return gantt_data
    
    def get_graph_data(self):
        """
        Generate graph structure for visualization.
        
        Returns:
            Dict with nodes and edges for graph rendering
        """
        nodes = []
        edges = []
        
        for task_name, task in self.tasks.items():
            nodes.append({
                "id": task_name,
                "label": task_name,
                "status": task.status.value,
                "duration": task.duration() if task.start_time else 0
            })
        
        for task_name, deps in self.dependencies.items():
            for dep in deps:
                edges.append({
                    "from": dep,
                    "to": task_name
                })
        
        return {"nodes": nodes, "edges": edges}
    
    def start_monitoring(self):
        """Start HTTP monitoring server"""
        class MonitoringHandler(BaseHTTPRequestHandler):
            executor = self
            
            def log_message(self, format, *args):
                """Suppress HTTP logs"""
                pass
            
            def do_GET(handler_self):
                if handler_self.path == '/':
                    handler_self.send_dashboard()
                elif handler_self.path == '/api/status':
                    handler_self.send_status()
                elif handler_self.path == '/api/gantt':
                    handler_self.send_gantt()
                elif handler_self.path == '/api/graph':
                    handler_self.send_graph()
                else:
                    handler_self.send_response(404)
                    handler_self.end_headers()
            
            def send_dashboard(handler_self):
                """Send HTML dashboard"""
                html = self._generate_dashboard_html()
                handler_self.send_response(200)
                handler_self.send_header('Content-Type', 'text/html')
                handler_self.end_headers()
                handler_self.wfile.write(html.encode())
            
            def send_status(handler_self):
                """Send current execution status"""
                stats = self.get_execution_stats()
                handler_self.send_response(200)
                handler_self.send_header('Content-Type', 'application/json')
                handler_self.end_headers()
                handler_self.wfile.write(json.dumps(stats).encode())
            
            def send_gantt(handler_self):
                """Send Gantt chart data"""
                gantt = self.get_gantt_data()
                handler_self.send_response(200)
                handler_self.send_header('Content-Type', 'application/json')
                handler_self.end_headers()
                handler_self.wfile.write(json.dumps(gantt).encode())
            
            def send_graph(handler_self):
                """Send graph structure"""
                graph = self.get_graph_data()
                handler_self.send_response(200)
                handler_self.send_header('Content-Type', 'application/json')
                handler_self.end_headers()
                handler_self.wfile.write(json.dumps(graph).encode())
        
        self.http_server = HTTPServer(('', self.monitoring_port), MonitoringHandler)
        self.server_thread = Thread(target=self.http_server.serve_forever, daemon=True)
        self.server_thread.start()
        
        print(f"📊 Dashboard: http://localhost:{self.monitoring_port}/")
    
    def stop_monitoring(self):
        """Stop monitoring server"""
        if self.http_server:
            self.http_server.shutdown()
    
    def _generate_dashboard_html(self):
        """Generate interactive HTML dashboard"""
        return """
<!DOCTYPE html>
<html>
<head>
    <title>Task Graph Executor - Dashboard</title>
    <meta http-equiv="refresh" content="1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 20px;
            box-shadow: 0 8px 16px rgba(0,0,0,0.3);
        }
        .header h1 { font-size: 32px; margin-bottom: 10px; }
        .header p { opacity: 0.9; font-size: 16px; }
        
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .card {
            background: #1e293b;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            border: 1px solid #334155;
        }
        
        .card-title {
            font-size: 14px;
            text-transform: uppercase;
            color: #94a3b8;
            margin-bottom: 12px;
            font-weight: 600;
            letter-spacing: 0.5px;
        }
        
        .metric {
            font-size: 48px;
            font-weight: bold;
            line-height: 1;
        }
        
        .metric.completed { color: #10b981; }
        .metric.running { color: #3b82f6; }
        .metric.failed { color: #ef4444; }
        .metric.pending { color: #94a3b8; }
        
        .task-list {
            background: #1e293b;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 20px;
            border: 1px solid #334155;
        }
        
        .task-item {
            display: flex;
            align-items: center;
            padding: 12px;
            margin-bottom: 8px;
            background: #0f172a;
            border-radius: 8px;
            border-left: 4px solid #334155;
        }
        
        .task-item.COMPLETED { border-left-color: #10b981; }
        .task-item.RUNNING { border-left-color: #3b82f6; }
        .task-item.FAILED { border-left-color: #ef4444; }
        .task-item.SKIPPED { border-left-color: #f59e0b; }
        
        .task-icon {
            font-size: 24px;
            margin-right: 16px;
            width: 30px;
            text-align: center;
        }
        
        .task-info { flex: 1; }
        .task-name { font-weight: 600; font-size: 16px; margin-bottom: 4px; }
        .task-meta { font-size: 13px; color: #94a3b8; }
        
        .progress-bar {
            height: 8px;
            background: #334155;
            border-radius: 4px;
            overflow: hidden;
            margin: 20px 0;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #10b981 0%, #3b82f6 100%);
            transition: width 0.3s ease;
        }
        
        .gantt {
            background: #1e293b;
            border-radius: 12px;
            padding: 24px;
            border: 1px solid #334155;
            overflow-x: auto;
        }
        
        .gantt-row {
            display: flex;
            align-items: center;
            margin-bottom: 12px;
            min-height: 40px;
        }
        
        .gantt-label {
            width: 150px;
            font-size: 14px;
            font-weight: 500;
        }
        
        .gantt-timeline {
            flex: 1;
            height: 32px;
            position: relative;
            background: #0f172a;
            border-radius: 4px;
        }
        
        .gantt-bar {
            position: absolute;
            height: 100%;
            border-radius: 4px;
            display: flex;
            align-items: center;
            padding: 0 8px;
            font-size: 12px;
            font-weight: 600;
        }
        
        .gantt-bar.COMPLETED { background: #10b981; }
        .gantt-bar.RUNNING { background: #3b82f6; animation: pulse 1s infinite; }
        .gantt-bar.FAILED { background: #ef4444; }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.7; }
        }
        
        .timestamp {
            text-align: center;
            color: #64748b;
            margin-top: 20px;
            font-size: 14px;
        }
    </style>
    <script>
        async function loadStatus() {
            const response = await fetch('/api/status');
            const data = await response.json();
            updateMetrics(data);
            updateTaskList(data);
        }
        
        async function loadGantt() {
            const response = await fetch('/api/gantt');
            const data = await response.json();
            updateGantt(data);
        }
        
        function updateMetrics(data) {
            document.getElementById('completed').textContent = data.completed;
            document.getElementById('running').textContent = data.running;
            document.getElementById('failed').textContent = data.failed;
            document.getElementById('total').textContent = data.total_tasks;
            
            const progress = (data.completed / data.total_tasks) * 100;
            document.getElementById('progress-fill').style.width = progress + '%';
            document.getElementById('progress-text').textContent = Math.round(progress) + '%';
        }
        
        function updateTaskList(data) {
            const container = document.getElementById('task-list');
            container.innerHTML = '';
            
            const icons = {
                'PENDING': '(Pending)',
                'READY': '(Ready)',
                'RUNNING': '(Running)',
                'COMPLETED': '(completed)',
                'FAILED': '(failed)',
                'SKIPPED': '(skipped)'
            };
            
            for (const [name, details] of Object.entries(data.task_details)) {
                const item = document.createElement('div');
                item.className = 'task-item ' + details.status;
                item.innerHTML = `
                    <div class="task-icon">${icons[details.status]}</div>
                    <div class="task-info">
                        <div class="task-name">${name}</div>
                        <div class="task-meta">
                            ${details.duration > 0 ? details.duration.toFixed(2) + 's' : 'Not started'}
                            ${details.retries > 0 ? ' • ' + details.retries + ' retries' : ''}
                            ${details.error ? ' • ' + details.error : ''}
                        </div>
                    </div>
                `;
                container.appendChild(item);
            }
        }
        
        function updateGantt(data) {
            if (data.length === 0) return;
            
            const container = document.getElementById('gantt');
            container.innerHTML = '<div class="card-title">Execution Timeline</div>';
            
            const maxTime = Math.max(...data.map(d => d.start + d.duration));
            
            data.forEach(task => {
                const row = document.createElement('div');
                row.className = 'gantt-row';
                
                const startPercent = (task.start / maxTime) * 100;
                const widthPercent = (task.duration / maxTime) * 100;
                
                row.innerHTML = `
                    <div class="gantt-label">${task.task}</div>
                    <div class="gantt-timeline">
                        <div class="gantt-bar ${task.status}" 
                             style="left: ${startPercent}%; width: ${widthPercent}%">
                            ${task.duration.toFixed(1)}s
                        </div>
                    </div>
                `;
                container.appendChild(row);
            });
        }
        
        // Auto-refresh
        setInterval(() => {
            loadStatus();
            loadGantt();
        }, 1000);
        
        // Initial load
        window.onload = () => {
            loadStatus();
            loadGantt();
        };
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1> Task Graph Executor</h1>
            <p>Real-time dependency-aware task execution monitoring</p>
        </div>
        
        <div class="grid">
            <div class="card">
                <div class="card-title">Completed</div>
                <div class="metric completed" id="completed">0</div>
            </div>
            <div class="card">
                <div class="card-title">Running</div>
                <div class="metric running" id="running">0</div>
            </div>
            <div class="card">
                <div class="card-title">Failed</div>
                <div class="metric failed" id="failed">0</div>
            </div>
            <div class="card">
                <div class="card-title">Total Tasks</div>
                <div class="metric pending" id="total">0</div>
            </div>
        </div>
        
        <div class="card">
            <div class="card-title">Overall Progress</div>
            <div class="progress-bar">
                <div class="progress-fill" id="progress-fill" style="width: 0%"></div>
            </div>
            <div style="text-align: center; margin-top: 8px; font-size: 24px; font-weight: bold;" id="progress-text">0%</div>
        </div>
        
        <div class="gantt" id="gantt">
            <div class="card-title">Execution Timeline</div>
            <p style="color: #64748b; margin-top: 10px;">Waiting for execution to start...</p>
        </div>
        
        <div class="task-list">
            <div class="card-title">Task Details</div>
            <div id="task-list"></div>
        </div>
        
        <div class="timestamp">
            Last updated: <span id="timestamp"></span> • Auto-refreshing every second
        </div>
    </div>
    
    <script>
        setInterval(() => {
            document.getElementById('timestamp').textContent = new Date().toLocaleTimeString();
        }, 1000);
    </script>
</body>
</html>
"""


# ============================================================================
# DEMO: Data Pipeline with Visualization
# ============================================================================

def demo_data_pipeline_with_viz():
    """Demonstrate task executor with real-time visualization"""
    
    print("=" * 70)
    print("TASK GRAPH EXECUTOR - Data Pipeline Demo with Visualization")
    print("=" * 70)
    
    # Simulate data pipeline tasks
    def extract_data(source, duration=1.5):
        """Extract data from source"""
        time.sleep(duration)
        return f"data_from_{source}"
    
    def transform_data(data, transformation, duration=2):
        """Transform data"""
        time.sleep(duration)
        return f"{data}_transformed_{transformation}"
    
    def load_data(data, destination, duration=1):
        """Load data to destination"""
        time.sleep(duration)
        return f"{data}_loaded_to_{destination}"
    
    def validate(data, duration=0.5):
        """Validate data"""
        time.sleep(duration)
        return f"{data}_validated"
    
    # Create executor with visualization
    executor = VisualizationExecutor(max_workers=3, monitoring_port=8090)
    
    # Build data pipeline graph
    # Extract from multiple sources
    executor.add_task(Task(
        "extract_users",
        extract_data,
        args=("users_db", 1.5)
    ))
    
    executor.add_task(Task(
        "extract_orders",
        extract_data,
        args=("orders_db", 2)
    ))
    
    executor.add_task(Task(
        "extract_products",
        extract_data,
        args=("products_api", 1)
    ))
    
    # Transform data
    executor.add_task(Task(
        "transform_users",
        transform_data,
        args=("users", "normalize"),
        kwargs={"duration": 1.5},
        dependencies=["extract_users"]
    ))
    
    executor.add_task(Task(
        "transform_orders",
        transform_data,
        args=("orders", "aggregate"),
        kwargs={"duration": 2.5},
        dependencies=["extract_orders"]
    ))
    
    executor.add_task(Task(
        "transform_products",
        transform_data,
        args=("products", "enrich"),
        kwargs={"duration": 1},
        dependencies=["extract_products"]
    ))
    
    # Validate transformed data
    executor.add_task(Task(
        "validate_users",
        validate,
        args=("users_transformed",),
        dependencies=["transform_users"]
    ))
    
    executor.add_task(Task(
        "validate_orders",
        validate,
        args=("orders_transformed",),
        dependencies=["transform_orders"]
    ))
    
    # Load to data warehouse
    executor.add_task(Task(
        "load_warehouse",
        load_data,
        args=("all_data", "warehouse"),
        kwargs={"duration": 2},
        dependencies=["validate_users", "validate_orders", "transform_products"]
    ))
    
    # Final validation
    executor.add_task(Task(
        "final_validation",
        validate,
        args=("warehouse_data", 1),
        dependencies=["load_warehouse"]
    ))
    
    # Start monitoring server
    executor.start_monitoring()
    
    print("\n Dashboard started!")
    print("   Open: http://localhost:8090/")
    print("   Watch tasks execute in real-time!\n")
    
    # Visualize before execution
    print(executor.visualize_graph())
    print()
    
    # Execute
    results = executor.execute()
    
    print("\n Keep the dashboard open to see the final timeline!")
    print("   Press Enter to stop monitoring server...")
    input()
    
    executor.stop_monitoring()


if __name__ == "__main__":
    demo_data_pipeline_with_viz()