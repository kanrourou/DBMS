#include "test_util.h"

int main()
{
	// Basic Functions
  cout << endl << "Create Tables ..." << endl;

  // Create Table tbl_employee
  createTable("tbl_employee");

  // Create Table tbl_employee2
	createTable("tbl_employee2");

  // Create Table tbl_employee3
  createTable("tbl_employee3");

  // Create Table tbl_employee4
  createLargeTable("tbl_employee4");

  return 0;
}
