FROM python:3.8-alpine

RUN pip install --user --upgrade pip

WORKDIR /app
COPY . .

RUN pip install pipenv
RUN pipenv sync --system

CMD ["pipenv","run", "python3", "main.py"]