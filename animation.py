import os

'''This runs ffmpeg, which get the images from images/ of type .png,
    then creates a video called out.mp4 with x images a second, as
    picked in framerate.'''
os.system('ffmpeg -framerate 2 -pattern_type glob -i "images/*.png" -vf "fps=25,format=yuv420p" out.mp4')

