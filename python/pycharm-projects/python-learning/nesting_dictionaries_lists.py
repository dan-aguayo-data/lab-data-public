
#dummy dictionary

capitals = {
    "France" : "Paris",
    "Germany" : "Berlin",
}

print(capitals)

travel_log = {
    "France": ["Paris","Lile","Dijon"],
    "Germany": ["Stuttgart","Berlin"],
}

#print dictionary
print(capitals)

#print list
print(travel_log)
print(travel_log["France"][1])

#Nested list
nested_list = ["A","B",["C","D"]]

print(nested_list[2]) #print the nest
print(nested_list[2][1]) #printe an item form the nest


travel_log_hybrid = {
    "France": {
        "total_visits" : 8,
        "cities_visited" : ["Paris","Lile","Champagne"],
    },
    "Germany": {
        "total_visits" : 5,
        "cities_visited" : ["Berlin","Hamburg","Stutgart"],
    } ,
}

print(travel_log_hybrid)
print(travel_log_hybrid["France"])
print(travel_log_hybrid["France"]["cities_visited"])
print(travel_log_hybrid["France"]["cities_visited"][0])