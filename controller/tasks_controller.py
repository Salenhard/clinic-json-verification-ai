from flask import Blueprint, jsonify, request

from repository import AbstractTaskRepository


def create_task_blueprint(repo: AbstractTaskRepository) -> Blueprint:
    tasks_bp = Blueprint("tasks", __name__, url_prefix="/api/tasks")

    @tasks_bp.get("/<task_id>")
    def get_task(task_id: str):
        task = repo.get(task_id)

        if task is None:
            return jsonify({"error": "Task not found"}), 404

        return jsonify(task.to_dict()), 200

    @tasks_bp.get("/")
    def get_tasks():
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 10))

        result = repo.get_all(page=page, page_size=page_size)

        return jsonify({
            "items": [task.to_dict() for task in result["items"]],
            "page": result["page"],
            "page_size": result["page_size"],
            "total": result["total"],
            "pages": result["pages"],
        }), 200

    @tasks_bp.delete("/<task_id>")
    def delete_task(task_id: str):
        try:
            repo.delete(task_id)
            return jsonify({"status": "deleted"}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 400

    return tasks_bp