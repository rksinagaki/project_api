FROM public.ecr.aws/lambda/python:3.11

WORKDIR /var/task

COPY requirements/requirements_lambda.txt .

RUN pip install --no-cache-dir -r requirements_lambda.txt

COPY src/app_lambda.py .

CMD ["app_lambda.lambda_handler"]