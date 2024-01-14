FROM python:3.10.13-slim

RUN python -m pip install --upgrade pip;\
    pip install kafka-python;\
    pip install pandas;

ADD prod.py /

CMD ["python", "./prod.py"]