"""
Tests for load_profiles — kiểm tra việc load và render Jinja2 trong profiles.yaml.

Sau khi fix, load_profiles chỉ render Jinja2 cho target được chọn,
không render tất cả targets → tránh crash khi target khác có env_var thiếu.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from cli.lib import load_profiles

PROFILES_PATH = Path(__file__).parent / "profiles.yaml"


class TestLoadProfilesBasicValidation:
    """Kiểm tra validation cơ bản: profile không tồn tại, target không tồn tại."""

    def test_invalid_profile_raises(self):
        """Profile không tồn tại trong profiles.yaml → raise Exception."""
        with pytest.raises(Exception, match="Profile .* not found"):
            load_profiles(
                PROFILES_PATH,
                project_name="test_project",
                profile_name="nonexistent_profile",
                target_name="dev",
            )

    def test_invalid_target_raises(self):
        """Target không tồn tại trong profile → raise Exception."""
        with pytest.raises(Exception, match="Target .* not found"):
            load_profiles(
                PROFILES_PATH,
                project_name="test_project",
                profile_name="sunhouse_etl_pipeline",
                target_name="nonexistent_target",
            )


class TestLoadProfilesSelectiveRendering:
    """
    Verify rằng load_profiles CHỈ render Jinja2 cho target được chọn,
    không render các target khác.
    """

    def test_dev_target_works_without_prod_env_vars(self):
        """
        FIX: Chọn target 'dev' mà KHÔNG cần set env vars cho prod.
        Dev target chỉ dùng env_var có default → phải pass.
        """
        # Đảm bảo DBT_USER và DBT_PASSWORD KHÔNG tồn tại
        env_clean = {
            k: v for k, v in os.environ.items()
            if k not in ("DBT_USER", "DBT_PASSWORD", "DBT_DATABASE",
                         "DBT_HOST", "DBT_PORT", "DBT_SCHEMA",
                         "DBT_TARGET", "DBT_THREADS")
        }
        with patch.dict(os.environ, env_clean, clear=True):
            result = load_profiles(
                PROFILES_PATH,
                project_name="test_project",
                profile_name="sunhouse_etl_pipeline",
                target_name="dev",
            )

        dev_target = result["sunhouse_etl_pipeline"]["outputs"]["dev"]
        assert dev_target["type"] == "trino"
        assert dev_target["method"] == "none"
        assert dev_target["user"] == "trino"
        # Default values được dùng đúng
        assert dev_target["host"] == "172.16.100.161"
        assert dev_target["port"] == 30346
        assert dev_target["database"] == "dp_landing_zone"
        assert dev_target["schema"] == "staging"
        assert dev_target["threads"] == 4

    def test_dev_target_with_env_override(self):
        """
        Khi set env vars, giá trị env var được dùng thay vì default.
        """
        mock_env = {
            "DBT_HOST": "custom_host",
            "DBT_PORT": "9999",
            "DBT_DATABASE": "custom_db",
            "DBT_SCHEMA": "custom_schema",
            "DBT_THREADS": "16",
        }
        env_clean = {
            k: v for k, v in os.environ.items()
            if not k.startswith("DBT_")
        }
        env_clean.update(mock_env)

        with patch.dict(os.environ, env_clean, clear=True):
            result = load_profiles(
                PROFILES_PATH,
                project_name="test_project",
                profile_name="sunhouse_etl_pipeline",
                target_name="dev",
            )

        dev_target = result["sunhouse_etl_pipeline"]["outputs"]["dev"]
        assert dev_target["host"] == "custom_host"
        assert dev_target["port"] == 9999
        assert dev_target["database"] == "custom_db"
        assert dev_target["schema"] == "custom_schema"
        assert dev_target["threads"] == 16

    def test_prod_target_not_rendered_when_dev_selected(self):
        """
        Khi chọn dev, prod target vẫn giữ nguyên raw Jinja2 string.
        """
        env_clean = {
            k: v for k, v in os.environ.items()
            if not k.startswith("DBT_")
        }
        with patch.dict(os.environ, env_clean, clear=True):
            result = load_profiles(
                PROFILES_PATH,
                project_name="test_project",
                profile_name="sunhouse_etl_pipeline",
                target_name="dev",
            )

        prod_target = result["sunhouse_etl_pipeline"]["outputs"]["prod"]
        # Prod target giữ nguyên raw Jinja2 string (chưa render)
        assert "{{ env_var('DBT_USER') }}" == prod_target["user"]
        assert "{{ env_var('DBT_PASSWORD') }}" == prod_target["password"]


class TestLoadProfilesJinjaFilters:
    """Kiểm tra Jinja2 filters hoạt động đúng (int, as_number, v.v.)."""

    def test_int_filter_converts_port_to_integer(self):
        """
        profiles.yaml dùng `| int` cho port và threads.
        Verify rằng giá trị trả về là int, không phải string.
        """
        env_clean = {
            k: v for k, v in os.environ.items()
            if not k.startswith("DBT_")
        }
        with patch.dict(os.environ, env_clean, clear=True):
            result = load_profiles(
                PROFILES_PATH,
                project_name="test_project",
                profile_name="sunhouse_etl_pipeline",
                target_name="dev",
            )

        dev_target = result["sunhouse_etl_pipeline"]["outputs"]["dev"]
        assert dev_target["port"] == 30346
        assert isinstance(dev_target["port"], int)
        assert dev_target["threads"] == 4
        assert isinstance(dev_target["threads"], int)


class TestLoadProfilesTargetResolution:
    """Kiểm tra target resolution khi target_name=None."""

    def test_default_target_from_profile(self):
        """
        FIX: Khi target_name=None, render trường 'target' trước để lấy
        tên target thực (ví dụ: 'dev') thay vì dùng raw Jinja2 string.
        """
        env_clean = {
            k: v for k, v in os.environ.items()
            if not k.startswith("DBT_")
        }

        with patch.dict(os.environ, env_clean, clear=True):
            result = load_profiles(
                PROFILES_PATH,
                project_name="test_project",
                profile_name="sunhouse_etl_pipeline",
                target_name=None,  # ← phải render trước rồi mới lookup
            )

        profile = result["sunhouse_etl_pipeline"]
        assert profile["target"] == "dev"
        # Verify dev target cũng được render đúng
        dev_target = profile["outputs"]["dev"]
        assert dev_target["type"] == "trino"
        assert dev_target["host"] == "172.16.100.161"

    def test_default_target_with_env_override(self):
        """
        Khi DBT_TARGET được set, target mặc định thay đổi theo.
        """
        mock_env = {
            "DBT_TARGET": "prod",
            # Phải set env vars cho prod target (không có default)
            "DBT_USER": "prod_user",
            "DBT_PASSWORD": "prod_pass",
            "DBT_DATABASE": "prod_db",
            "DBT_HOST": "prod_host",
        }
        env_clean = {
            k: v for k, v in os.environ.items()
            if not k.startswith("DBT_")
        }
        env_clean.update(mock_env)

        with patch.dict(os.environ, env_clean, clear=True):
            result = load_profiles(
                PROFILES_PATH,
                project_name="test_project",
                profile_name="sunhouse_etl_pipeline",
                target_name=None,  # ← sẽ resolve thành 'prod'
            )

        profile = result["sunhouse_etl_pipeline"]
        assert profile["target"] == "prod"
        prod_target = profile["outputs"]["prod"]
        assert prod_target["type"] == "trino"
        assert prod_target["user"] == "prod_user"
        assert prod_target["database"] == "prod_db"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
