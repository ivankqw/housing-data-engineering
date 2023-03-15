FROM apache/airflow:2.5.1
COPY requirements.txt .
ENV TZ=Asia/Singapore
RUN pip install -r requirements.txt
RUN ipython kernel install --name "dataengineering" --user