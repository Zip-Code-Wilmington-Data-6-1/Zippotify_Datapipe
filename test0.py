# Python
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from fast_api import app, get_db
from models import DimUser

import pytest

client = TestClient(app)

# Helper: Fake user instance
def fake_user():
    user = MagicMock(spec=DimUser)
    user.user_id = 1
    user.gender = "M"
    user.registration_ts = 1234567890
    user.birthday = 946684800
    return user

# Fixture to override get_db dependency
@pytest.fixture
def override_get_db():
    def _override(users):
        def _get_db():
            db = MagicMock()
            db.query.return_value.all.return_value = users
            db.query.return_value.filter.return_value.first.side_effect = (
                lambda: users[0] if users else None
            )
            yield db
        app.dependency_overrides[get_db] = _get_db
    yield _override
    app.dependency_overrides.clear()

def test_get_users(override_get_db):
    users = [fake_user()]
    override_get_db(users)
    response = client.get("/users")
    assert response.status_code == 200
    assert response.json() == [{
        "user_id": 1,
        "gender": "M",
        "registration_ts": 1234567890,
        "birthday": 946684800
    }]

def test_get_user_found(override_get_db):
    users = [fake_user()]
    override_get_db(users)
    response = client.get("/users/1")
    assert response.status_code == 200
    assert response.json() == {
        "user_id": 1,
        "gender": "M",
        "registration_ts": 1234567890,
        "birthday": 946684800
    }

def test_get_user_not_found(override_get_db):
    users = []
    override_get_db(users)
    response = client.get("/users/1")
    assert response.status_code == 200
    assert response.json() == {}