
#Create encryption method that converts a letter to anothe in the alphapet depending on a given shift
alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z']


def caesarcipher(operation,original_text,shift):
    cipher_text = ""
    if operation == 'encrypt':
        word = 'encoded'
        for letter in original_text:
            if letter not in alphabet:
                cipher_text += letter
            else:
                shifted_position = alphabet.index(letter) + shift
                shifted_position %= len(alphabet) #modulo tell us how many I am off by 1/25 to 25/25 has no remainder
                cipher_text += alphabet[shifted_position]
    elif operation == 'decrypt':
        word = 'decoded'
        for letter in original_text:
            if letter not in alphabet:
                cipher_text += letter
            else:
                shifted_position = alphabet.index(letter) - shift
                shifted_position %= len(alphabet)
                cipher_text += alphabet[shifted_position]
    else:
        print("Please only choose type 'encrypt' or decrypt'")
    print(f"Here is the {word} text: {cipher_text}\n")

should_continue = True

while should_continue:

    operation = input("Type  encrypt or decrypt:\n").lower()
    text = input("Type your message:\n").lower()
    shift = int(input("Type the shift number:\n"))
    caesarcipher(operation=operation,original_text=text,shift= shift)
    should_continue = input("Do you want to continue?Yes/No\n")

#Issue of going out of range in the alphabet is resolved by using module to restart from the beginning