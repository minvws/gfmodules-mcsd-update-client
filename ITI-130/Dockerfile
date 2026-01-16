# docker build -t iti130-publisher /home/yob/ITI-130
# docker run --name ITI-130-publisher --rm --network mcsd_update_client_net iti130-publisher

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY iti130_publisher.py ./

ENV SQL_CONN=sqlite:///demo.db \
    FHIR_BASE=http://hapi-directory:8080/fhir

ENTRYPOINT ["python", "iti130_publisher.py"]
# set the profile to NL, always resets the sqlite db seed, and includes the practitioners
CMD ["--profile-set", "nl", "--sqlite-reset-seed", "--include-practitioners", "--fhir-reset-seed"]
# CMD ["--profile-set", "nl", "--include-practitioners", "--dry-run", "--out", "bundle.json"]
