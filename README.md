# pixel-histograms-scala
Generate Histogram for each RGB color in the given dataset using Scala and Spark

Project done as a part of CSE-6331 Cloud Computing Course at UTA.

<a href="https://lambda.uta.edu/cse6331/spring20/project4.html">Project Description</a>

A pixel in an image can be represented using 3 colors: red, green, and blue, where each color intensity is an integer between 0 and 255. In this project, you are asked to write a Map-Reduce program that derives a histogram for each color. For red, for example, the histogram will indicate how many pixels in the dataset have a green value equal to 0, equal to 1, etc (256 values). The pixel file is a text file that has one text line for each pixel.

For example, the line

23,140,45 represents a pixel with red=23, green=140, and blue=45.

<p>Same project as <a href="https://github.com/c-deshpande/pixel-histograms">this</a> but implemented in Scala</p>
