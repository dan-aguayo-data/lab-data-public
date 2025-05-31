import json
import random
from art import text2art
import pyfiglet
from time import sleep


directory = r'C:\Users\DanielAguayo\lab\python\pycharm-projects\python-learning\sources\data_higher_lower.json'



def logo_generator(text ):
    art_text = text2art(text, font="slant")
    output = art_text
    return output

def json_random_generator(file_path):
    with open(file_path, "r", encoding="utf-8") as jsonfile:
        # Load the JSON data
        data = json.load(jsonfile)
        # max_element = max(data, key=lambda x: x['keyword'])
        # last_element = data[-1]
        lenght_dictionary = len(data)
    random_number = random.randrange(0, lenght_dictionary)
    random_dictionary = data[random_number]
    random_keyword = random_dictionary['keyword']
    random_searchVolume = random_dictionary['searchVolume']

    return random_number, random_dictionary, random_keyword, random_searchVolume

def json_random_generator_excl(file_path,number_excl):
    with open(file_path, "r", encoding="utf-8") as jsonfile:
        data = json.load(jsonfile)
        lenght_dictionary = len(data)
    while True:
        random_number = random.randrange(0, lenght_dictionary)
        if random_number != number_excl:
            break
    random_dictionary = data[random_number]
    random_keyword = random_dictionary['keyword']
    random_searchVolume = random_dictionary['searchVolume']

    return random_number, random_dictionary, random_keyword, random_searchVolume



def play_higuerlower():
    play_again = True
    while play_again == True:
        win_status = True
        logo = logo_generator("HigherLower")
        vs_logo = logo_generator("VS.")
        score =  0
        print(logo)
        print("Welcome to the higher or lower game! ")
        print('\n'*1)
        sleep(2)
        print("You are going to guess which of the two options has a higher popularity, if you get it right you get 1 point, if not, you lose.")
        sleep(5)
        print('\n'*1)
        random_number_init,random_dictionary_init, random_keyword_init, random_searchVolume_init = json_random_generator(directory)
        number = random_number_init
        keyword = random_keyword_init
        searchVolume = random_searchVolume_init


        while win_status is True:
            random_number_vs, random_dictionary_vs, random_keyword_vs, random_searchVolume_vs = json_random_generator_excl(directory,number)
            print("What has more popularity?")
            sleep(5)
            print('\n' * 1)
            print(f"Option A. {keyword}.")
            print('\n' * 1)
            sleep(2)
            print(vs_logo)
            # print('\n' * 1)
            sleep(2)
            print(f"Option B. {random_keyword_vs}.")
            print('\n' * 1)
            sleep(2)
            choice = input("Type 'A' or 'B': ")
            A = searchVolume
            B = random_searchVolume_vs
            if A > B:
                right_choice = 'A'
            else:
                right_choice = 'B'
            if choice.upper() == right_choice:
                score += 1
                print('\n'*1)
                sleep(3)
                print(f"You are right! Current Score: {score}.")
                print('\n' * 1)
                if right_choice == 'B':
                    number = random_number_vs
                    keyword = random_keyword_vs
                    searchVolume = random_searchVolume_vs

            else:
                win_status = False
                print('\n'*1)
                sleep(4)
                print(f"You are wrong!")
                sleep(4)
                print(f"Final Score: {score}.")
                print('\n'*1)
                sleep(4)
                score = 0
                play_again_input = input("Do you wish to play again? 'Yes' or 'No': ")
                if play_again_input.upper() == 'YES':
                    play_again = True
                else:
                    play_again = False



play_higuerlower()









