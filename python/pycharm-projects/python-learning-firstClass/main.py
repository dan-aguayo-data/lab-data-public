################CLASSS##########################
#class syntax, naming ideally should follow PascalCase .. Cap first letter of a word.
#camelcase has the first word lower case only
#snake case all letter are separated by _

# attribute = what the object has. Basically the variables inside init
# method = what the obect does. Basically a function inside the class

###################init#########################
# this will be called every time I create a new object form the class
# after self.. add a "," and put the parameter you want to link to attributes



class User:
    def __init__(self,user_id, user_name):
        self.id = user_id  # id is the attribute
        self.username = user_name  #ideally parameter = attribute in name
        self.followers = 0  # you can also make an attribute with a deterministic value
        self.following = 0

    # pass   # ignore for the time being
    def follow(self,user):    #always self, + normal func params
        user.followers += 1
        self.following += 1


user_1 = User("001","Angela")  ## object from class
print(user_1.id)
user_2 = User("002","Daniel")

user_1.follow(user_2)

print(user_1.followers)
print(user_1.following)
print(user_2.followers)
user_1.name = "Angela"  #this is an attribute of the object


# Initializing the variable is the way we start the construction of our variables.
# This is done under the class with _init_

