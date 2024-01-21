FROM python:3.10.13-slim

RUN python -m pip install --upgrade pip;\
    pip install kafka-python;\
    pip install pandas;\
    pip install requests;\
    pip install beautifulsoup4;

ADD dwd_crawler.py /
ADD producer.py /

CMD ["python", "./producer.py"]