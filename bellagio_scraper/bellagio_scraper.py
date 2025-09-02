import json
import requests
import re
from datetime import datetime
from dateutil import parser
import re
import json

PROPERTY_ID = "44e610ab-c209-4232-8bb4-51f7b9b13a75"

ENTERTAINMENT_TAX_MULTIPLIER = 0.09

SERVICE_TAX_MULTIPLIER = 0.09

MAX_RETRY_COUNT = 3

AUTH_TOKEN = ""

venue_url = "https://api.mgmresorts.com/graphql-next"
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
            # url = "https://api.mgmresorts.com/graphql-next"

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

            response = requests.post(venue_url, headers=headers, json=data)

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
                print("Auth token expired. Retrying...")
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
                            "show_id": show.get("eventId",""),
                            "event_name": event_name.replace('"',''),
                            "event_date": event_date,
                            "event_time": event_time,
                            "event_code": show.get("eventCode",""),
                            "season_id": show.get("seasonId",""),
                            "event_url": base_url.format(event_id, show.get("eventId",""))
                        })
                    except Exception as e:
                        print(f"An exception has occurred while processing the show {event_name}-{event_date} {event_time}:{e}")
                break
            elif response.status_code == 401:
                retry_count += 1
                print("Auth token expired. Retrying...")
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
                print("Auth token expired. Retrying...")
                get_auth_token()
            else:
                raise Exception(f"Request to fetch event details failed with status code {response.status_code}")
        else:
            print("Max retry reached for extracting event details.")   
        
    except Exception as e:
        print("An exception has occurred while extracting event"+str(e))

    return service_charge_amount, seasons_dict

def get_all_events( start_date, end_date):
    all_events_list = []
    try:
        main_events = get_events()
        for event_obj in main_events:
            try:
                service_charge, seasons_dict = get_event_details(event=event_obj)
                shows = get_shows(event=event_obj, start_date=start_date, end_date=end_date)
                if not shows:
                    raise Exception("No shows found.")
                for show in shows:
                    try:
                        season_id = show.get("season_id","")
                        season_obj = seasons_dict.get(season_id,{})

                        show["service_charge"] = service_charge
                        show["seating_type"] = season_obj.get("seating_type","")
                        show["max_tickets"] = season_obj.get("max_tickets",1)

                        all_events_list.append(show)

                    except Exception as e:
                        print(f"An exception occurred while adding show to all events list: {e}")
            except Exception as e:
                print(f"An exception occurred while adding shows data to all events list for event {event_obj.get('event_name','')}")


    except Exception as e:
        print("An exception has occurred while extracting all events"+str(e))
    return all_events_list

def get_seats_data(event):
    retry_count = 0

    try:
        while retry_count < MAX_RETRY_COUNT:
            seats_data = {}
            show_id = event.get("show_id","")
            url = "https://api.mgmresorts.com/graphql-next?q=GetSeatsAvailability"

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
                query GetSeatsAvailability($showEventId: String!, $programId: String, $numTicket: Int, $sections: String, 
                                        $priceCodes: String, $redemptionCode: String, $numAdaTickets: Int, 
                                        $numCompanionTickets: Int, $offerSeatsOnly: Boolean, $propertyId: String) {
                showBooking {
                    seatsAvailability(queryParams: {
                    showEventId: $showEventId, 
                    programId: $programId, 
                    numTickets: $numTicket, 
                    sections: $sections, 
                    priceCodes: $priceCodes, 
                    redemptionCode: $redemptionCode, 
                    numAdaTickets: $numAdaTickets, 
                    numCompanionTickets: $numCompanionTickets, 
                    offerSeatsOnly: $offerSeatsOnly, 
                    propertyId: $propertyId
                    }) {
                    limitedAvailability
                    comp
                    hdePackage
                    seats {
                        count
                        startingPrice
                        name
                        seatRows {
                        name
                        seats {
                            firstSeat
                            lastSeat
                            numSeats
                            seatIncrement
                            priceCode
                            basePrice
                            discountedPrice
                            priceLevel
                            holdClass
                            ada
                            adaCompanion
                        }
                        }
                    }
                    prices {
                        code
                        description
                        price {
                        basePrice
                        discountedPrice
                        comp
                        hdePackage
                        discounted
                        }
                        minTickets
                        maxTickets
                        totalAvailableSeats
                        ticketTypeCode
                        ticketTypeDescription
                        offerApplicable
                    }
                    seatRowsSortedByPrice {
                        name
                        sectionName
                        startingPrice
                        numSeats
                    }
                    }
                }
                }
                """,
                "variables": {
                    "showEventId": show_id,
                    "propertyId": PROPERTY_ID,
                    "offerSeatsOnly": False
                },
                "operationName": "GetSeatsAvailability"
            }

            response = requests.post(url, headers=headers, json=data)

            if response.status_code == 200:
                print("Request to fetch seats data is successfull.")
                seats_data = response.json()
                break
            elif response.status_code == 401:
                print("Auth token expired. Retrying...")
                retry_count += 1
                get_auth_token()    
            else:
                raise Exception(f"Request to fetch seats data failed with status code: {response.status_code}")
        else:
            print("Max retry reached for extracting event seats data.")

    
    except Exception as e:
        print("An exception has occurred while extracting seats data."+str(e))


    return seats_data

def get_regular_seats(event, venue):
    regular_seats = []
    price_dict = {}
    try:

        show_id = event.get("show_id","")
        event_name = event.get("event_name","")
        event_date = event.get("event_date","")
        event_time = event.get("event_time","")
        service_charge = event.get("service_charge",0)


        seats_data = get_seats_data(event=event)

        if seats_data:
            seats_availability = seats_data.get("data",{}).get("showBooking",{}).get("seatsAvailability",{})

            prices = seats_availability.get("prices",[])

            price_dict = {price.get("code",""): {"available_seats":price.get("totalAvailableSeats",0),"price_desc":price.get("description",0),"price_value":price.get("price",{}).get("basePrice",0), "price_type":price.get("ticketTypeDescription","")} for price in prices}

            seats_obj = seats_availability.get("seats",[])

            for section_obj in seats_obj:
                try:
                    section_name = section_obj.get("name","")
                    rows = section_obj.get("seatRows",[])
                    if any(kw in section_name.lower().strip() for kw in ['vip']):
                        continue
                    for row in rows:
                        try:
                            row_name = row.get("name","")
                            seats = row.get("seats",[])
                            for seat in seats:
                                try:
                                    is_ada = seat.get("ada",True)
                                    is_companion = seat.get("adaCompanion", True)

                                    if is_ada or is_companion:
                                        continue
                                    price_code = seat.get("priceCode","")
                                    price_obj = price_dict.get(price_code,{})

                                    price = price_obj.get("price_value",0)
                                    price_desc = price_obj.get("price_desc","")

                                    if any(kw in price_desc.lower().strip() for kw in ['accessible','wheelchair','limited view','companion']):
                                        continue

                                    if price == 0:
                                        continue

                                    if service_charge > 0:
                                        price = round(price + service_charge + price*ENTERTAINMENT_TAX_MULTIPLIER + service_charge*SERVICE_TAX_MULTIPLIER ,2)

                                    num_seats = seat.get("numSeats",0)

                                    first_seat = seat.get("firstSeat",0)
                                    last_seat = seat.get("lastSeat",0)
                                    seat_increment = seat.get("seatIncrement",1)

                                    if num_seats == 1:
                                        regular_seats.append({
                                            "Venue Name": venue,
                                            "Event Name": event_name,
                                            "Event Date": event_date,
                                            "Event Time": event_time,
                                            "Section": section_name,
                                            "Row": row_name,
                                            "Seat": first_seat,
                                            "Price": price,
                                            "Desc":"",
                                            "UniqueIdentifier": show_id,
                                            "TimeStamp": datetime.now().strftime("%d %b %Y %H:%M:%S")

                                        })
                                    elif num_seats > 1:
                                        for seat_num in range(first_seat, last_seat+1, seat_increment):
                                            regular_seats.append({
                                                "Venue Name": venue,
                                                "Event Name": event_name,
                                                "Event Date": event_date,
                                                "Event Time": event_time,
                                                "Section": section_name,
                                                "Row": row_name,
                                                "Seat": seat_num,
                                                "Price": price,
                                                "Desc":"",
                                                "UniqueIdentifier": show_id,
                                                "TimeStamp": datetime.now().strftime("%d %b %Y %H:%M:%S")

                                        })

                                    else:
                                        continue
                                except:
                                    pass
                        except:
                            pass
                except Exception as e:
                    print(f"An exception has occurred while extracting seats from section {section_name}")
    except Exception as e:
        print("An exception has occurred while extracting regular seats."+str(e))


    return regular_seats

def get_ga_seats(event, venue):
    ga_seats = []
    price_dict = {}
    try:
        event_id = event.get("event_id","")
        show_id = event.get("show_id","")
        event_name = event.get("event_name","")
        event_date = event.get("event_date","")
        event_time = event.get("event_time","")
        service_charge = event.get("service_charge",0)
        seating_type = event.get("seating_type","")

        seats_data = get_seats_data(event=event)
        if seats_data:
            seats_availability = seats_data.get("data",{}).get("showBooking",{}).get("seatsAvailability",{})

            prices = seats_availability.get("prices",[])

            for price_obj in prices:
                price_desc = price_obj.get("description","").lower().strip() 

                if any(kw in price_desc for kw in ['general admission','general']):
                    if not price_desc in price_dict:
                        price_dict["ga"] = price_obj
                    else:
                        current_price = price_obj.get("price",{}).get("basePrice",0)
                        existing_price = price_dict.get(price_desc,{}).get("price",{}).get("basePrice",0)

                        if current_price > existing_price:
                            price_dict["ga"] = price_obj

            if not price_dict:
                raise Exception("Price data not found.")
            
            price_obj = price_dict.get("ga",0)

            price = price_obj.get("price",{}).get("basePrice",0)

            if price == 0:
                raise Exception("Price is 0.")

            available_seats = price_obj.get("totalAvailableSeats",0)


            if service_charge > 0:
                price = round(price + service_charge + price*ENTERTAINMENT_TAX_MULTIPLIER + service_charge*SERVICE_TAX_MULTIPLIER ,2)

            ga_seats.append({
                "Venue Name": venue,
                "Event Name": event_name,
                "Event Date": event_date,
                "Event Time": event_time,
                "Section": "General Admission",
                "Row": "GA",
                "Seat": "",
                "Price": price,
                "Desc": f"{available_seats}-max seats",
                "UniqueIdentifier": show_id,
                "TimeStamp": datetime.now().strftime("%d %b %Y %H:%M:%S")
            })

    except Exception as e:
        print('An exception has occurred while extracting ga seats.'+str(e))
    return ga_seats


def scrape_event(venue_name, event_unique_id, start_date, end_date):
    global MAX_RETRY_COUNT
    seats_data = []
    try:
        if not event_unique_id:
            raise Exception("Event unique id not provided.")

        get_auth_token()

        if not AUTH_TOKEN:
            raise Exception("Auth token not found.")

        events = get_all_events(start_date=start_date, end_date=end_date)

        if not events:
            raise Exception("No events found.")
        
        target_event = None
        for event in events:
            if str(event.get("show_id")) == event_unique_id:
                target_event = event
                break
        
        if not target_event:
            raise Exception(f"Event ID {event_unique_id} not found")
        
        print("Event found")

        seating_type = target_event.get("seating_type","").lower().strip()

        if seating_type == "seatselection":
            seats_data = get_regular_seats(event=target_event, venue=venue_name)
        elif seating_type == "generaladmission":
            seats_data = get_ga_seats(event=target_event, venue=venue_name)
        else:
            raise Exception(f"Unknown seating type: {seating_type}")
        
        if not seats_data:
            raise Exception("No seats found")

        return json.dumps({
            "status": "success",
            "message": f"Successfully extracted {len(seats_data)} seats",
            "event_data": seats_data
        })
             
    except Exception as e:
        print("An exception has occurred while scraping the event.")
        return json.dumps({
            "status": "error", 
            "message": f"An exception occurred while scraping the event: {str(e)}"
        }) 


# output = scrape_event("Bellagio","Event_559431","2025-09-14","2025-09-14")

# print(output)