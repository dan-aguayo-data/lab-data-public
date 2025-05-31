#dictionaries help us group and tag pieces of information
# every dictionary has two parts 1) Key 2) Associated value

#Syntax for the dictionary, and use the format below when working with these type of arrays
programming_dictionary ={
    "Bug":"An error in the program that prevetns the program from running as expected",
    "Other":"Test"
}

print(programming_dictionary["Bug"])

#Add additional values to the dictionary

programming_dictionary["New Value"] = "This is a new line"

print(programming_dictionary)

#Wipe an existing dictionary by
programming_dictionary = {}

print(programming_dictionary)


programming_dictionary ={
    "Bug":"An error in the program that prevetns the program from running as expected",
    "Other":"Test Data for the dictionary"
}
#looping through the keys and their values of a dictionary

for key in programming_dictionary:
    print(key)
    print(programming_dictionary[key])

