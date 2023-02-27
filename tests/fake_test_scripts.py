"""
Модуль с тестами
Чтобы получить coverage report нужно запуститть

pytest --cov && coverage xml -o coverage-reports/coverage.xml

Потенциально важно
coverage==4.5.4
pytest-mock==3.3.1
Вроде как не важно
pytest==6.0.2
pytest-cov==2.10.1
"""
from scripts.fake_test import inc


def test_answer():
    assert inc(3) == 4
