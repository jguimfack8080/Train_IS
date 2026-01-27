FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Par défaut, on garde le conteneur en vie pour pouvoir lancer des commandes (train/predict)
CMD ["tail", "-f", "/dev/null"]
