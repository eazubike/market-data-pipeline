"""
Lambda entry point for the FastAPI app using Mangum adapter.
Deploy as a Lambda function behind API Gateway or Function URL.
"""

from mangum import Mangum
from api.main import app

handler = Mangum(app, lifespan="off")
