import geoip2.database;
import pprint

def get_city(ip):
    with geoip2.database.Reader("./ip_databases/GeoLite2-City.mmdb") as reader:
        response = reader.city(ip)
        return response.city.names["en"]

def get_country(ip):
    with geoip2.database.Reader("./ip_databases/GeoLite2-City.mmdb") as reader:
        response = reader.city(ip)
        return response.registered_country.names["en"]

def get_asn(ip):
    with geoip2.database.Reader("./ip_databases/GeoLite2-ASN.mmdb") as reader:
        response = reader.asn(ip)
        return response.autonomous_system_organization
        