"""Validation tests for demo-control API payloads."""

import pytest
from pydantic import ValidationError

from agent_fleet.server import (
    AgentDisconnectRequest,
    CustomerChangeRequest,
    DriverDisconnectRequest,
)


@pytest.mark.parametrize("driver_id", ["driver-a", "driver-b", "driver-c", "driver-d", "driver-e"])
def test_driver_request_accepts_known_drivers(driver_id: str):
    assert DriverDisconnectRequest(driver_id=driver_id).driver_id == driver_id


def test_driver_request_rejects_unknown_driver():
    with pytest.raises(ValidationError):
        DriverDisconnectRequest(driver_id="driver-z")


@pytest.mark.parametrize("agent_name", ["fleet_agent", "customer_agent"])
def test_agent_request_accepts_supported_agents(agent_name: str):
    assert AgentDisconnectRequest(agent_name=agent_name).agent_name == agent_name


def test_agent_request_rejects_unknown_agent():
    with pytest.raises(ValidationError):
        AgentDisconnectRequest(agent_name="dispatch_agent")


@pytest.mark.parametrize("change_type", ["address_change", "cancel"])
def test_customer_change_accepts_supported_types(change_type: str):
    request = CustomerChangeRequest(order_id="order-1", change_type=change_type)
    assert request.change_type == change_type


def test_customer_change_rejects_unknown_type():
    with pytest.raises(ValidationError):
        CustomerChangeRequest(order_id="order-1", change_type="refund")
