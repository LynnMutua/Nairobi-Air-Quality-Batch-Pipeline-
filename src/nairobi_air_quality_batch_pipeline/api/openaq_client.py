import requests

class OpenAQClient:
    def __init__ (self, api_key: str, host: str):

        self.base_url = f"https://{host}/v3"
        self.session = requests.Session()

        self.session.headers.update({
            "X-API-Key": api_key,
            "Accept":"application/json",
        }) 

    def get_locations(
        self, 
        latitude: float= -1.278560, 
        longitude: float= 36.821249,
        radius: int =25000,
        limit: int =100,

    ) -> list[dict]:
        response = self.session.get(
            f"{self.base_url}/locations",
            params={
                "coordinates": f"{latitude}, {longitude}",
                "radius": radius,
                "limit": limit,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("results",[])
    

    def get_sensors(self, location_id: int, limit: int = 100) -> list[dict]:
        response = self.session.get(
            f"{self.base_url}/locations/{location_id}/sensors",
            params={
                "limit": limit, 
                "location_id": location_id,},
            timeout=30,
        )

        response.raise_for_status()

        payload = response.json()
        return payload.get("results", [])


    
    def get_measurements(
        self, 
        sensor_id: int, 
        datetime_from: str = None, 
        datetime_to: str = None, 
        limit: int = 100
    ) -> list[dict]:
        
        params = {"limit": limit}
        if datetime_from:
            params["datetime_from"] = datetime_from
        if datetime_to:
            params["datetime_to"] = datetime_to

        response = self.session.get(
            f"{self.base_url}/sensors/{sensor_id}/measurements",
            params=params,
            timeout=30,
        )
        
        response.raise_for_status()
        payload = response.json()
        return payload.get("results", [])






        







