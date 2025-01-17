/* Copyright (GPL) 2004 mchirico@users.sourceforge.net or mchirico@comcast.net

  Simple lemon parser example.
  
    $ ./lemon gramma.y                          

  The above statement will create gramma.c.

  The command below  adds  main and the
  necessary "Parse" calls to the
  end of this gramma.c.


    $ cat <<EOF >>gramma.c                      
    int main()                                    
    {                                             
      void* pParser = ParseAlloc (malloc);        
      Parse (pParser, INTEGER, 1);                
      Parse (pParser, PLUS, 0);                   
      Parse (pParser, INTEGER, 2);                
      Parse (pParser, 0, 0);                      
      ParseFree(pParser, free );                  
     }                                            
    EOF                                           

     $ g++ -o gram gramma.c                                      
     $ ./gram

  See the Makefile, as most all of this is
  done automatically.
  
  Downloads:
	http://prdownloads.sourceforge.net/souptonuts/lemon_examples.tar.gz?download
*/

%token_type {int}

%left PLUS MINUS. 
%left DIVIDE TIMES.

%include {
#include <iostream>
#include "gramma.h"
}  

%syntax_error {
  std::cout << "Syntax error!" << std::endl;
}
  
program ::= expr(A).   { std::cout << "Result=" << A << std::endl; }

expr(A) ::= expr(B) MINUS  expr(C).   { A = B - C; }
expr(A) ::= expr(B) PLUS  expr(C).   { A = B + C; } 
expr(A) ::= expr(B) TIMES  expr(C).   { A = B * C; }
expr(A) ::= expr(B) DIVIDE expr(C).  {

         if(C != 0){
           A = B / C;
         }else{
           std::cout << "divide by zero" << std::endl;
         }
}  /* end of DIVIDE */

expr(A) ::= INTEGER(B). { A = B; }
