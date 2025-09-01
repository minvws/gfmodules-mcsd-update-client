import json
from unittest.mock import patch, mock_open
from fastapi.testclient import TestClient


def test_index_with_version_file(api_client: TestClient) -> None:
    version_data = {"version": "1.2.3", "git_ref": "abc123"}
    
    with patch("builtins.open", mock_open(read_data=json.dumps(version_data))):
        response = api_client.get("/")
        
    assert response.status_code == 200
    assert "Version: 1.2.3" in response.text
    assert "Commit: abc123" in response.text
    assert "___  ________  ___________" in response.text

def test_version_json_with_file(api_client: TestClient) -> None:
    version_content = '{"version": "1.2.3", "git_ref": "abc123"}'
    
    with patch("builtins.open", mock_open(read_data=version_content)):
        response = api_client.get("/version.json")
        
    assert response.status_code == 200
    assert response.text == version_content

def test_index_without_version_file(api_client: TestClient) -> None:
    with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
        response = api_client.get("/")
        
    assert response.status_code == 200
    assert "No version information found" in response.text
    assert "___  ________  ___________" in response.text

def test_version_json_without_file(api_client: TestClient) -> None:
    with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
        response = api_client.get("/version.json")
        
    assert response.status_code == 404