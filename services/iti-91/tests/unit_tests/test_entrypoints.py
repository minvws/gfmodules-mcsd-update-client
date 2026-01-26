from unittest.mock import MagicMock

from app import create_db
from app import main


def test_app_main_should_call_application_run(monkeypatch) -> None:
    mock_run = MagicMock()
    monkeypatch.setattr(main.application, "run", mock_run)

    main.main()

    mock_run.assert_called_once()


def test_create_db_main_should_call_generate_tables(monkeypatch) -> None:
    mock_db = MagicMock()
    mock_get_database = MagicMock(return_value=mock_db)
    mock_application_init = MagicMock()

    monkeypatch.setattr(create_db.container, "get_database", mock_get_database)
    monkeypatch.setattr(create_db.application, "application_init", mock_application_init)

    create_db.main()

    mock_application_init.assert_called_once()
    mock_get_database.assert_called_once()
    mock_db.generate_tables.assert_called_once()
