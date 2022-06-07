package com.challenge.support

/**
 * This is a trait.
 * 
 * All class which are responsible to read the file, those must extends from this trait.
 * 
 */
trait ReadDataFileT {
  
  
  def readDataFile(): Array[String];
}