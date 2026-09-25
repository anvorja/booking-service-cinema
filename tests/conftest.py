import os

# Settings exige estas variables al importarse; las pruebas unitarias no tocan
# base de datos ni Kafka.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("JWT_SECRET", "test-secret")
