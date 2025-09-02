import json
import requests
from dateutil import parser
import re

PROPERTY_ID = "44e610ab-c209-4232-8bb4-51f7b9b13a75"

ENTERTAINMENT_TAX_MULTIPLIER = 0.09

SERVICE_TAX_MULTIPLIER = 0.09

MAX_RETRY_COUNT = 3

AUTH_TOKEN = ""

url = "https://api.mgmresorts.com/graphql-next"
base_url = "https://mandalaybay.mgmresorts.com/book-show/{}/seats/?event={}"

def get_auth_token():
    global AUTH_TOKEN
    try:
        url = "https://identityapi.mgmresorts.com/identity/authorization/v1/anon/user/token"

        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Origin": "https://bellagio.mgmresorts.com",
            "Referer": "https://bellagio.mgmresorts.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36",
            "content-type": "application/json",
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print("Request to fetch auth token is successfull.")
            data = response.json()

            AUTH_TOKEN = data.get("access_token","")
        else:
            raise Exception(f"Request to fetch auth token failed with status code {response.status_code}")

    except Exception as e:
        print("An exception has occurred while fetching auth token."+str(e))
                
def get_events():
    retry_count = 0

    try:
        while retry_count < MAX_RETRY_COUNT:
            events_list = []
            headers = {
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Origin": "https://bellagio.mgmresorts.com",
                "Referer": "https://bellagio.mgmresorts.com/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36",
                "accept": "*/*",
                "authorization": f"Bearer {AUTH_TOKEN}",
                "content-type": "application/json",
                "x-mgm-channel": "web",
                "x-mgm-source": "mgmri"
            }

            data = {
                "operationName": "SearchCategory",
                "variables": {
                    "params": {
                        "propertyId": PROPERTY_ID,
                        "category": "entertainment",
                        "limit": 32,
                        "offset": 0,
                        "options": [
                            {
                                "key": "propertyId_s",
                                "type": "multiselect",
                                "value": [PROPERTY_ID]
                            }
                        ]
                    }
                },
                "query": "query SearchCategory($params: CategorySearchParams!) {\n  searchCategory(params: $params) {\n    total\n    fusionQueryId\n    results {\n      id\n      coverImage {\n        alt\n        src\n        __typename\n      }\n      bookingURL\n      targetURL\n      name\n      detailA\n      detailB\n      detailC\n      detailD\n      open\n      __typename\n    }\n    facetFields {\n      name\n      value {\n        key\n        occurrences\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}"
            }

            response = requests.post(url, headers=headers, json=data)

            if response.status_code == 200:
                events_json_data = response.json()
                print("request to fetch events successfull.")

                results = events_json_data.get("data",{}).get("searchCategory",{}).get("results",[])
                for result in results:
                    try:
                        id = result.get("id","")
                        event_id = id.split("/")[-1]
                        name = result.get("name","")
                        clean_text = re.sub(r"[^\x00-\x7F]+", "", name) 
                        event_name = clean_text

                        if event_id:
                            events_list.append({
                                "event_id": event_id,
                                "event_name": event_name
                            })
                    except Exception as e:
                        print("An exception has occurred while processing event.")
                # with open("SanPedro/events.json","w") as f:
                #     json.dump(events_list,f, indent=4)
                break
            elif response.status_code == 401:
                retry_count += 1
                get_auth_token()

            else:
                print(f"Request to fetch events failed with status code {response.status_code}")
                raise Exception(f"Request to fetch events failed with status code {response.status_code}")
        else:
            print("Max retry reached for extracting events.")
     
    except Exception as e:
        print("An exception occurred while fetching events.")

    return events_list

def get_shows(event, start_date, end_date):
    retry_count = 0

    try:
        while retry_count < MAX_RETRY_COUNT:
            shows_list = []
            event_id = event.get("event_id","")
            event_name = event.get("event_name","")

            url = "https://api.mgmresorts.com/graphql-next?q=GetEventsAvailabilityForShow"

            headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Content-Type": "application/json",
                "Origin": "https://bellagio.mgmresorts.com",
                "Referer": "https://bellagio.mgmresorts.com/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36",
                "authorization": f"Bearer {AUTH_TOKEN}",
                "x-mgm-channel": "web",
                "x-mgm-source": PROPERTY_ID
            }

            payload = {
                "query": """
                query GetEventsAvailabilityForShow($endDate: String, $limit: Int!, $offset: Int!, $propertyId: String!, $showId: String!, $startDate: String!) {
                showBooking {
                    eventsAvailabilityForShow(
                    queryParams: {endDate: $endDate, limit: $limit, offset: $offset, propertyId: $propertyId, showId: $showId, startDate: $startDate}
                    ) {
                    eventDate
                    eventTime
                    eventId
                    eventCode
                    seasonId
                    offerAvailable
                    }
                }
                }
                """,
                "variables": {
                    "propertyId": PROPERTY_ID,
                    "showId": event_id,
                    "startDate": start_date,
                    "endDate": end_date,
                    "limit": 500,
                    "offset": 0
                },
                "operationName": "GetEventsAvailabilityForShow"
            }

            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 200:
                print("Request to fetch shows is successfull.")
                shows_data = response.json()

                shows = shows_data.get("data",{}).get("showBooking",{}).get("eventsAvailabilityForShow",[])

                for show in shows:
                    try:
                        event_date = show.get("eventDate","")
                        event_time = show.get("eventTime","")

                        parsed_datetime = parser.parse(event_date+" "+event_time)
                        event_time = parsed_datetime.strftime("%H:%M:%S")

                        shows_list.append({
                            "event_id":event_id,
                            "event_unique_id": show.get("eventId",""),
                            "event_name": event_name.replace('"','',),
                            "event_date": event_date,
                            "event_time": event_time,
                            "event_url": base_url.format(event_id, show.get("eventId",""))
                        })
                    except Exception as e:
                        print(f"An exception has occurred while processing the show {event_name}-{event_date} {event_time}:{e}")
                break
            elif response.status_code == 401:
                retry_count += 1
                get_auth_token()

            else:
                raise Exception(f"Request to fetch shows failed with status code {response.status_code}")
        else:
            print("Max retry reached for extracting shows.")

    except Exception as e:
        print("An exception has occurred while extracting shows"+str(e))

    return shows_list

def get_event_details(event):
    retry_count = 0

    try:
        while retry_count < MAX_RETRY_COUNT:
            service_charge_amount = 0
            seasons_dict = {}
            event_id = event.get("event_id","")

            url = "https://api.mgmresorts.com/graphql-next?q=GetShow"

            headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Content-Type": "application/json",
                "Origin": "https://bellagio.mgmresorts.com",
                "Referer": "https://bellagio.mgmresorts.com/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36",
                "Authorization": f"Bearer {AUTH_TOKEN}",
                "x-mgm-channel": "web",
                "x-mgm-source": PROPERTY_ID
            }

            data = {
                "query": """
                query GetShow($showId: String, $eventId: String) {
                    show(id: $showId, eventId: $eventId) {
                        id
                        ageRequirements
                        allDayEvent
                        arenaTheater {
                            name
                            boxOfficeHours {
                                description
                                time
                            }
                            location {
                                address
                            }
                        }
                        boxOfficeHours {
                            description
                            time
                        }
                        dailyHours {
                            temporaryClosed
                            monday {
                                ...HoursFragment
                            }
                            tuesday {
                                ...HoursFragment
                            }
                            wednesday {
                                ...HoursFragment
                            }
                            thursday {
                                ...HoursFragment
                            }
                            friday {
                                ...HoursFragment
                            }
                            saturday {
                                ...HoursFragment
                            }
                            sunday {
                                ...HoursFragment
                            }
                        }
                        bookingFlowGroupReservation
                        darkDates
                        descriptions {
                            appCard
                            overview
                            learnMore
                            long
                            secondary
                            short
                            tagline
                        }
                        exceptionalHours {
                            exceptionDate
                            firstClosingTime
                            firstOpeningTime
                            is24Hours
                            isClosed
                            secondClosingTime
                            secondOpeningTime
                        }
                        groupReservations {
                            title
                            phoneNumber
                            email
                            moreInfo {
                                text
                                url
                            }
                        }
                        images {
                            tile {
                                url
                                metadata {
                                    description
                                }
                            }
                            detailPageHeroGallery {
                                type
                                reference {
                                    url
                                }
                                caption
                                placeHolderImage {
                                    url
                                }
                            }
                            overview {
                                url
                                metadata {
                                    description
                                    height
                                    width
                                }
                            }
                        }
                        lastAdmissionTime
                        name
                        productCategory
                        property {
                            id
                            name
                            location {
                                latitude
                                longitude
                                address
                                directions {
                                    title
                                    directions
                                }
                                description
                                mapImage
                            }
                            phoneNumber {
                                generalNumber
                            }
                            regionName
                            timezone
                        }
                        propertyId
                        priceRange
                        seasons {
                            id
                            minTickets
                            maxTickets
                            eventTimesDescription
                            useSeatMap
                            periodEndDate
                            seatMap {
                                seatMapReference
                                seatingType
                                venueSeatMapImage {
                                    url
                                    metadata {
                                        width
                                        height
                                        description
                                    }
                                }
                                seatMapVenue {
                                    ...SeatMapVenueFragment
                                }
                            }
                        }
                        showTimesDescriptions {
                            description
                            time
                        }
                        startingPrice
                        startingPriceDiscounted
                        ticketPricingDescription
                        startingPriceWithServiceFees
                        serviceChargeAmount
                    }
                }

                fragment HoursFragment on OpeningHours {
                    closing1
                    closing2
                    hoursType
                    opening1
                    opening2
                }

                fragment SeatMapVenueFragment on SeatMapVenue {
                    backgroundImage {
                        url
                    }
                    backgroundImageSvg {
                        url
                    }
                    seatMapCoordinatesFile
                    seatMapCoordinatesSvg {
                        url
                    }
                    canvasHoverSvg {
                        url
                    }
                }
                """,
                "variables": {
                    "showId": event_id
                },
                "operationName": "GetShow"
            }

            response = requests.post(url, headers=headers, json=data)

            if response.status_code == 200:
                print("Request to fetch event details is successfull.")
                show_data = response.json()

                show = show_data.get("data",{}).get("show",{})

                service_charge_amount = show.get("serviceChargeAmount",0)

                seasons = show.get("seasons",[])

                seasons_dict = {season.get("id",""): {"max_tickets": season.get("maxTickets",1), "seating_type": season.get("seatMap",{}).get("seatingType","")} for season in seasons}

                break
            
            elif response.status_code == 401:
                retry_count += 1
                get_auth_token()
            else:
                raise Exception(f"Request to fetch event details failed with status code {response.status_code}")
        else:
            print("Max retry reached for extracting event details.") 
            raise Exception(f"Max retry reached for extracting event details for event {event.get('event_name','')}")
        
    except Exception as e:
        print("An exception has occurred while extracting event"+str(e))
        raise Exception(f"An exception has occurred while extracting event details for event {event.get('event_name','')}: {e}")

    return service_charge_amount, seasons_dict

def get_list_of_events(start_date, end_date):
    all_events_list = []
    try:
        main_events = get_events()
        for event_obj in main_events:
            try:
                shows = get_shows(event=event_obj, start_date=start_date, end_date=end_date)
                if not shows:
                    raise Exception("No shows found.")

                all_events_list.extend(shows)
                 
            except Exception as e:
                print(f"An exception occurred while adding shows data to all events list for event {event_obj.get('event_name','')}")


    except Exception as e:
        print("An exception has occurred while extracting all events"+str(e))

    return all_events_list


# output = get_list_of_events("2025-08-13","2027-03-20")

# print(json.dumps(output, indent=2))
