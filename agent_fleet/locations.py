"""
Single source of truth for all map locations — Las Vegas Strip.

Ice cream commissary kitchen + hotel delivery destinations.
"""

from agent_fleet.models import Coords

# Commissary kitchen — east of the strip near Convention Center
WAREHOUSE = Coords(lat=36.1280, lng=-115.1520)
WAREHOUSE_LABEL = "Ice Cream Kitchen"

# Hotel delivery destinations on the Las Vegas Strip (3 hotels for clean demo)
DELIVERY_DESTINATIONS = {
    "order-1": {
        "hotel": "MGM Grand",
        "label": "MGM Grand pool party — 150 servings",
        "coords": Coords(lat=36.1024, lng=-115.1696),
        "map_label": "MGM Grand",
        "priority": "vip",
        "servings": 150,
        "deadline_minutes": 35,
    },
    "order-2": {
        "hotel": "Caesars Palace",
        "label": "Caesars Palace VIP banquet — 100 servings",
        "coords": Coords(lat=36.1162, lng=-115.1745),
        "map_label": "Caesars",
        "priority": "vip",
        "servings": 100,
        "deadline_minutes": 30,
    },
    "order-3": {
        "hotel": "Mandalay Bay",
        "label": "Mandalay Bay conference — 80 servings",
        "coords": Coords(lat=36.0919, lng=-115.1761),
        "map_label": "Mandalay Bay",
        "priority": "vip",
        "servings": 80,
        "deadline_minutes": 30,
    },
}

# Deterministic crew assignments — 1 order per crew for easy visual tracking
CREW_ASSIGNMENTS = {
    "ai-crew-1": ["order-1"],  # MGM Grand
    "ai-crew-2": ["order-2"],  # Caesars Palace
    "ai-crew-3": ["order-3"],  # Mandalay Bay
}
