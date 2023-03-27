#!/usr/bin/python
# This script is a simple guessing game. Used to demo 'while True'
# Version 1.0


print("Guess the number game. Guess a number between 1 and 1,000,000.")
print()

secret_number = 100
counter = 1

while True:
  guess = int(input("Enter a number between: 1 and 1,000,000 :> "))
  
  #less than 1, exit
  if guess < 1:
    print("You guess badly. Exiting")
    exit()
  #guess less than secret_number
  if guess < secret_number:
    print("Guess is too  low. Try again!")
    counter += 1
  #guess greater than secret_number
  elif guess > secret_number:
    print("Guess is too high. Try again!")
    counter += 1
    continue
  #guess is secret_number
  elif guess == secret_number:
    print("You win!")
    break 
  else:
    print("Unknown.")
print("You tried: ", counter, "time(s) to guess:", secret_number)
