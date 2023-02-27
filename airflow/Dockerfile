FROM apache/airflow:2.5.1
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install --upgrade pip ipython ipykernel
RUN ipython kernel install --name "dataengineering" --user