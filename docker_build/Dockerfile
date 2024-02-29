#FROM nginx:latest
FROM python:latest
WORKDIR /user/src/project_runpython
COPY try2/. .
COPY requirements.txt .
#COPY runpyshell.sh .
#COPY shell/. .
#COPY test.conf /etc/nginx/conf.d/
RUN pip3 install -r requirements.txt
#RUN chmod +x start_py.sh \
#	&& ./start_py.sh
EXPOSE 5000 8080 80
ENTRYPOINT ["python3","test2.py"]
#CMD echo "test2.py正在运行！"
#RUN chmod +x runpyshell.sh \
#	&& ./runpyshell.sh