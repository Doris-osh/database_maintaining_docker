#FROM nginx:latest
FROM python:latest
WORKDIR /user/src/project_runpython
COPY try2/. /codes/
COPY shell/. /
#COPY test.conf /etc/nginx/conf.d/
RUN pip3 install flask gevent gunicorn akshare numpy pandas mysql.connector sqlalchemy datetime chinese_calendar
RUN chmod +x start_py.sh \
	&& ./start_py.sh
EXPOSE 5000 8080 80