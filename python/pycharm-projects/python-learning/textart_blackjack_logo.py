from art import text2art
import pyfiglet
# print(pyfiglet.getFonts())

def blackjack_logo():
    blackjack_text = text2art("Blackjack", font="slant")
    print(blackjack_text)

    cards_ascii = """
      .------.  .------.
      |A_  _ |  |K  _  |
      |( \\/ )|  | (♥)  |
      | \\  / |  |(_'_) |
      |  \\/ A|  |  |  K|
      `------'  `------'
    """


    #creates an array/list for each line
    cards_lines = cards_ascii.splitlines()
    blackjack_lines = blackjack_text.splitlines()

    # Ensure both have the same number of lines by padding shorter one
    max_lines = max(len(cards_lines), len(blackjack_lines))
    cards_lines += [" " * len(cards_lines[0])] * (max_lines - len(cards_lines))
    blackjack_lines += [""] * (max_lines - len(blackjack_lines))
    #multiply {""} will create multiple lines inside the list


    # Print them side by side ... zip is a standard function that combines two lists element by element
    output = "\n".join(f"{card_line}  {blackjack_line}" for card_line, blackjack_line in zip(cards_lines, blackjack_lines))
    return output
