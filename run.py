# -*- coding: utf-8 -*-
"""容器/本地启动入口：python run.py

注意：不要加 ``-I``。隔离模式会从 sys.path 中去掉脚本所在目录，导致无法
导入同目录的 ``app`` 包；Dockerfile / scripts 均已使用普通 ``python``。
"""

import os
import sys

# 兼容在任意工作目录启动（例如 ``python /app/run.py``）
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app  # noqa: E402

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
