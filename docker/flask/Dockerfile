FROM tiangolo/uwsgi-nginx-flask:python3.12
COPY ./flask_app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
COPY ./flask_app/app /app
