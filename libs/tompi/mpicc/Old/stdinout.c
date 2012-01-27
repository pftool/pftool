#include <ctype.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hash.h"
#include "tokens.h"

#define IDENT_LEN 512
#define TYPE_LEN 65536
#define TYPE2_LEN 1024
#define MAX_ENTRY 100
#define MAX_BRACKETS 50

HashTab_list scopes;

char dummy_char1 = 'T', dummy_char2 = 'N';
char *is_type = &dummy_char1, *is_nothing = &dummy_char2;
void conditional_free (char *s)
{
    if (s != is_type && s != is_nothing)
        free (s);
}

extern int column, line;
extern char yytext[], last_yytext[];

#define warn(string) fprintf (stderr, "[%d:%d] Warning: %s\n", line, column, string)
#define start_warn(string) fprintf (stderr, "[%d-%d:%d] Warning: %s\n", start_line, line, column, string)

void lappend (char *, char *);
int read_until_inbalance (char *);
void replace (void);
extern void flush_out (void);

void main (void)
{
   /* Parser */
   int brace_level = 0, paren_level = 0, start_paren_level, start_brace_level;
   int token;
   enum {NONE, SPECIFIERS, ENTRY, INITIALIZER} state = SPECIFIERS;
   char seen_type = 0, seen_name = 0, eat_next_rparen = 0;

   /* Temps */
   char typename[IDENT_LEN];

   /* Memory */
   char is_static = 0, is_auto = 0, is_extern = 0, is_const = 0, is_typedef = 0;
   char type[TYPE_LEN] = "";
   int nent = 1;
   struct
   {
      char name[IDENT_LEN], type2[TYPE2_LEN];
      int nstar, npstar, nparens;
      char is_const;
   } ent[MAX_ENTRY];
   strcpy (ent[0].name, "<no name!>");
   ent[0].type2[0] = '\0';
   ent[0].nstar = ent[0].npstar = ent[0].nparens = 0;
   ent[0].is_const = 0;

   scopes = hashlist_new ();
   hashlist_push (scopes, hash_new ());

   printf ("extern void *_get_global (void **, int, void *); ");

   while (1)
   {
      token = yylex ();
      if (token <= 0)
          break;
      switch (state)
      {
         case NONE: state_NONE: state = NONE;
            switch (token)
            {
               case SEMI:
                  if (paren_level > 0)          /* for loops */
                     goto not_a_start;
                  break;
               case LBRACE:
                  brace_level++;
                  hashlist_push (scopes, hash_new ());
                  break;
               case RBRACE:
                  if (brace_level-- == 0)
                  {
                     warn ("} without matching { -- ignoring");
                     brace_level = 0;
                  }
                  hashlist_destroy_one (scopes, conditional_free);
                  break;
               case LPAREN:
                  paren_level++;
                  break;
               case RPAREN:
                  if (paren_level-- == 0)
                  {
                     warn (") without matching ( -- ignoring");
                     paren_level = 0;
                  }
                  break;
               case IDENTIFIER:
                  replace ();
                  /* goto not_a_start */
               default:
                  goto not_a_start;
            }
            state = SPECIFIERS;
            is_static = is_auto = is_extern = is_const = is_typedef = 0;
            type[0] = '\0';
            seen_type = seen_name = 0;
            nent = 1;
            strcpy (ent[0].name, "<no name!>");
            ent[0].type2[0] = '\0';
            ent[0].nstar = ent[0].npstar = ent[0].nparens = 0;
            ent[0].is_const = 0;
            start_paren_level = paren_level;
            start_brace_level = brace_level;
not_a_start:
            break;
         case SPECIFIERS: state_SPECIFIERS: state = SPECIFIERS;
            switch (token)
            {
               case TYPEDEF:
                  is_typedef = 1;
                  break;
               case EXTERN:
                  is_extern = 1;
                  break;
               case STATIC:
                  if (is_auto)
                     warn ("static after auto -- assuming static");
                  is_static = 1;
                  break;
               case AUTO:
                  if (is_static)
                     warn ("auto after static -- assuming auto");
                  is_auto = 1;
                  break;
               case REGISTER:
                  break;
               case CONST: case VOLATILE:
                  /* Consider a volatile variable constant in the sense that
                   * making it thread-specific data is nuts, unless it is
                   * just the thing being pointed to that is const or volatile.
                   */
                  lappend (type, yytext);
                  is_const = 1;
                  break;
               case VOID: case CHAR: case SHORT: case INT: case LONG:
               case FLOAT: case DOUBLE:
                  lappend (type, yytext);
                  seen_type = 1;
                  break;
               case SIGNED: case UNSIGNED:
                  lappend (type, yytext);
                  break;
               case IDENTIFIER:
                  if (hashlist_find (scopes, yytext) == is_type)
                  {
                      lappend (type, yytext);
                      if (seen_type && !is_typedef)
                          warn ("Multiple types for single declaration");
                      else
                          seen_type = 1;
                  }
                  else if (!seen_type)  /* Function call or assignment */
                      goto state_NONE;
                  else                  /* Variable name in declaration */
                      goto state_ENTRY;
                  break;
               case MULT_OP: case LPAREN:
                  if (!seen_type)       /* Executable statement */
                     goto state_NONE;
                  goto state_ENTRY;
               case STRUCT: case UNION: case ENUM:
               {
                  lappend (type, yytext);

                  if (seen_type == 1 && !is_typedef)
                     warn ("Multiple types for single declaration");
                  else
                     seen_type = 1;
                  if ((token = yylex ()) == IDENTIFIER)
                  {
                     strcpy (typename, yytext);
                     token = yylex ();
                  }
                  else
                     typename[0] = '\0';
                  if (token == LBRACE)
                  {
                     int start_line = line;
                     lappend (type, yytext);
                     if (read_until_inbalance (type) != RBRACE)
                     {
                        start_warn ("Got lost looking for } ending struct/union/enum -- skipping declaration");
                        state = NONE;
                     }

                     /* Make the struct/union/enum type a bonified type.
                      * This is not consistent with C (you can actually have
                      * a variable with the same name), but it is with
                      * C++ (e.g., class).
                      */
                     if (typename[0] != '\0')
                        hashlist_add (scopes, typename, is_type);
                  }
                  else
                  {
                      lappend (type, typename);
                      goto state_SPECIFIERS;
                  }
                  break;
               }
               default:
                  goto state_NONE;
            }
            break;
         case ENTRY: state_ENTRY: state = ENTRY;
            switch (token)
            {
               case COMMA:
                  seen_name = 0;
                  strcpy (ent[nent].name, "<no name!>");
                  ent[nent].type2[0] = '\0';
                  ent[nent].nstar = ent[nent].npstar = ent[nent].nparens = 0;
                  ent[nent].is_const = 0;
                  nent++;
                  break;
               case MULT_OP:    /* pointer */
                  lappend (ent[nent-1].type2, yytext);
                  if (seen_name)
                      warn ("`*' after the variable name");
                  if (paren_level > 0)
                      ent[nent-1].npstar++;
                  else
                      ent[nent-1].nstar++;
                  if ((token = yylex ()) == CONST || token == VOLATILE)
                  {
                      ent[nent-1].is_const = 1;
                      do {
                          lappend (ent[nent-1].type2, yytext);
                          token = yylex ();
                      } while (token == CONST || token == VOLATILE);
                  }
                  else
                      ent[nent-1].is_const = 0;
                  goto state_ENTRY;
               case EQUALS:
                  state = INITIALIZER;
                  break;
               case SEMI:
               {
                  int i;
                  if (is_typedef)
                     for (i = 0; i < nent; i++)
                        hashlist_add (scopes, ent[i].name, is_type);
                  else
                  {
                      flush_out ();

                      for (i = 0; i < nent; i++)
                                          /* Account for: */
                          if ((brace_level == 0 || is_static) &&
                                          /* Locals */
                                  !(ent[i].nparens > 0 && ent[i].npstar == 0) &&
                                          /* Function prototypes */
                                  !(is_const && ent[i].nstar <= 0) &&
                                          /* Constant (not pointer to one) */
                                  !ent[i].is_const)
                                          /* Constant pointer */
                          {
                              char *s = (char *) malloc (sizeof (char) *
                                      (3 * strlen (ent[i].name) + 35 +
                                       strlen (type) + strlen (ent[i].type2)));
                              if (s == NULL)
                              {
                                  perror ("g2tsd");
                                  exit (1);
                              }
                              sprintf (s, "(*((%s %s)_get_global(&_%s,sizeof(%s),&%s)))",
                                      type, ent[i].type2, ent[i].name,
                                      ent[i].name, ent[i].name);
                              hashlist_add (scopes, ent[i].name, s);

                              if (is_extern)
                                  printf (" extern void*_%s;", ent[i].name);
                              else
                                  printf (" void*_%s=(void*)0;", ent[i].name);
                          }
                          else
                              /* If we are overriding this variable from a
                               * different scope, define it as nothing to
                               * (if needed) undefine it as a global variable.
                               */
                              hashlist_add (scopes, ent[i].name, is_nothing);
                  }

                  /* Initialize and go to SPECIFIERS state. */
                  goto state_NONE;
               }
               case IDENTIFIER:
                  if (seen_name)
                      warn ("Two names for a variable; using latter one");
                  else
                  {
                      int len = strlen (ent[nent-1].type2);
                      if (len > 0 && ent[nent-1].type2[len-1] == '(')
                      {
                          eat_next_rparen = 1;
                          ent[nent-1].type2[len-1] = '\0';
                      }
                      else
                          eat_next_rparen = 0;
                      lappend (ent[nent-1].type2, "*");
                  }
                  seen_name = 1;
                  strcpy (ent[nent-1].name, yytext);
                  break;
               case LPAREN:
                  lappend (ent[nent-1].type2, yytext);
                  if (seen_name)
                  {
                     int start_line = line;
                     if (read_until_inbalance (ent[nent-1].type2) != RPAREN)
                     {
                        start_warn ("Got lost looking for ) ending parameter list -- skipping declaration");
                        state = NONE;
                     }
                     ent[nent-1].nparens++;
                  }
                  else
                     paren_level++;
                  break;
               case RPAREN:
                  if (eat_next_rparen)
                      eat_next_rparen = 0;
                  else
                      lappend (ent[nent-1].type2, yytext);
                  if (paren_level-- == 0)
                  {
                     warn (") without matching ( -- ignoring");
                     paren_level = 0;
                  }
                  /* Here we catch things like:
                   *    typedef void (*foo) (int i);
                   *                         ^^^^^*
                   * This would only happen if ( was a character like ; and {.
                   */
                  if (paren_level < start_paren_level)
                     state = NONE;
                  break;
               case LBRACKET:
               {
                  int start_line = line;
                  lappend (ent[nent-1].type2, "*");
                  if (read_until_inbalance (NULL) != RBRACKET)
                  {
                     start_warn ("Got lost looking for ] ending array dimension -- skipping declaration");
                     state = NONE;
                  }
                  break;
               }
               default:
                  goto state_NONE;
            }
            break;
         case INITIALIZER: state_INITIALIZER: state = INITIALIZER;
            switch (token)
            {
               case LBRACE:
                  brace_level++;
                  break;
               case RBRACE:
                  if (brace_level-- == 0)
                  {
                     warn ("} without matching { -- ignoring");
                     brace_level = 0;
                  }
                  break;
               case LPAREN:
                  paren_level++;
                  break;
               case RPAREN:
                  if (paren_level-- == 0)
                  {
                     warn (") without matching ( -- ignoring");
                     paren_level = 0;
                  }
                  break;
               case COMMA: case SEMI:
                  if (paren_level == start_paren_level &&
                      brace_level == start_brace_level)
                     goto state_ENTRY;
                  break;
            }
            break;
      }
   }

   flush_out ();
   fprintf (stderr, "Done!\n");
}

void replace (void)
{
    char *info = hashlist_find (scopes, yytext);
    if (info != NULL && info != is_type && info != is_nothing)
    {
        last_yytext[0] = '\0';
        printf ("%s", info);
    }
}

void lappend (char *s, char *add)
{
    if (s == NULL)
        return;
    if (*s == '\0')
        strcpy (s, add);
    else
    {
        int cnt = 0;
        while (*s != '\0')
        {
            cnt++;
            s++;
        }
        *(s++) = ' ';
        while (*add != '\0')
        {
            cnt++;
            if (cnt >= TYPE2_LEN)
            {
                fprintf (stderr, "Type too long; increase TYPE_LEN\n");
                exit (1);
            }
            *(s++) = *(add++);
        }
        *s = '\0';
    }
}

int read_until_inbalance (char *addto)
{
   int parens = 0, brackets = 0, braces = 0, done = 0;
   int token;

   while (1)
   {
      token = yylex ();
      if (token <= 0)
          return -1;
      lappend (addto, yytext);
      switch (token)
      {
         case LPAREN: parens++; break;
         case RPAREN: if (parens-- <= 0) done = 1; break;
         case LBRACKET: brackets++; break;
         case RBRACKET: if (brackets-- <= 0) done = 1; break;
         case LBRACE: braces++; break;
         case RBRACE: if (braces-- <= 0) done = 1; break;
      }
      if (done)
          return token;
   }
}

