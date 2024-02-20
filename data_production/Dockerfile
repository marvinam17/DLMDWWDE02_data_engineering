FROM python:3.10.13-slim

RUN python -m pip install --upgrade pip;\
    pip install kafka-python;\
    pip install pandas;\
    pip install requests;\
    pip install beautifulsoup4;

WORKDIR /code

ADD dwd_crawler.py /code
ADD producer.py /code
ADD ./sample_data /code/sample_data

CMD ["python", "./producer.py"]