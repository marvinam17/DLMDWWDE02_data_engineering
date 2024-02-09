FROM python:3.10.13-slim

RUN python -m pip install --upgrade pip;\
    pip install requests;
 
ADD import_dashboard.py /
ADD dashboard.zip /

CMD ["python", "./import_dashboard.py"]