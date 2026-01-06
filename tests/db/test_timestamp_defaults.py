import time
from datetime import datetime

from app.db.entities.resource_map import ResourceMap
from app.db.db import Database


def test_resource_map_timestamp_defaults_should_not_be_frozen(database: Database) -> None:
    with database.get_db_session() as session:
        rm1 = ResourceMap(
            directory_id="dir-1",
            resource_type="Organization",
            directory_resource_id="org-1",
            update_client_resource_id="uc-1",
        )
        session.add(rm1)
        session.commit()
        created_1 = rm1.created_at
        assert isinstance(created_1, datetime)

        time.sleep(0.01)

        rm2 = ResourceMap(
            directory_id="dir-1",
            resource_type="Organization",
            directory_resource_id="org-2",
            update_client_resource_id="uc-2",
        )
        session.add(rm2)
        session.commit()
        created_2 = rm2.created_at
        assert isinstance(created_2, datetime)

    assert created_2 > created_1
