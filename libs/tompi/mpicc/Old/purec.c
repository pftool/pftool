#include <ctype.h>
#include <stdio.h>
#include <string.h>

#define MAX_LEN 512
#define BIG_LEN 256000
#define MAX_DECL 64
#define MAX_BRACKETS 5

typedef struct
{
   char c[MAX_LEN+1];
   int n;
} char_array;

typedef struct
{
   char c[BIG_LEN+1];
   int n;
} big_char_array;

typedef enum {DEFAULT, NO, YES} answer;

typedef struct
{
   char_array name;
   char_array brackets[MAX_BRACKETS];
   int nbrackets;
} entry;

#define reset(string) do { \
                         fprintf (stderr, "[%d] Warning: %s; ignoring\n", lineno, string); \
                         state = NONE; \
                      } while (0)

#define static_automatic_string(val) ((val) == YES ? "static " : ((val) == NO ? "automatic " : ""))

#define NNONDECL_KEYWORDS 3
char *nondecl_keywords[NNONDECL_KEYWORDS] = {"return", "goto", "typedef"};
#define NCOMPOUND_KEYWORDS 4
char *compound_keywords[NCOMPOUND_KEYWORDS] = {"struct", "union", "enum", "class"};
#define NMODIFIER_KEYWORDS 2
char *modifer_keywords[NMODIFIER_KEYWORDS] = {"static", "automatic"};

int read_until_inbalance_small (char_array *);
int read_until_inbalance (big_char_array *);
int is (char_array *s, char *s2);
int in (char_array *s, char **list, int n);

static int lineno;

void main (void)
{
   int c;
   int bracelevel = 0;
   enum {NONE, READY_FOR_DECL, COMPOUND_TYPE, EATING_TYPE, AFTER_TYPE,
      EATING_NAME, AFTER_NAME, DID_BRACKET, AFTER_COMMA} state = READY_FOR_DECL;
   char_array type;
   big_char_array block;
   entry entries[MAX_DECL+1], *currentry;
   int nentry;
   answer is_static;
   char is_compound;

   lineno = 1;

   while ((c = fgetc (stdin)) != EOF)
   {
      if (isalnum (c) || c == '_')      /* \w */
      {
         switch (state)
         {
            case READY_FOR_DECL:
               type.n = 1;
               type.c[0] = c;
               is_static = DEFAULT;
               is_compound = 0;
               state = EATING_TYPE;
               break;
            case COMPOUND_TYPE:
               type.c[type.n++] = ' ';
               type.c[type.n++] = c;
               state = EATING_TYPE;
               break;
            case EATING_TYPE:
               if (type.n >= MAX_LEN)
                  reset ("Type name too long");
               else
                  type.c[type.n++] = c;
               break;
            case AFTER_TYPE:
               nentry = 1;
               entries[0].name.n = 1;
               entries[0].name.c[0] = c;
               entries[0].nbrackets = 0;
               currentry = entries;
               state = EATING_NAME;
               break;
            case EATING_NAME:
               if (currentry->name.n >= MAX_LEN)
                  reset ("Variable name too long");
               else
                  currentry->name.c[currentry->name.n++] = c;
               break;
            case AFTER_NAME: case DID_BRACKET:
               state = NONE;
               break;
            case AFTER_COMMA:
               if (nentry >= MAX_DECL)
                  reset ("Declaration too long");
               else
               {
                  entries[nentry].name.n = 1;
                  entries[nentry].name.c[0] = c;
                  entries[nentry].nbrackets = 0;
                  currentry++;
                  nentry++;
                  state = EATING_NAME;
               }
            case NONE:
         }
      }
      else if (isspace (c))             /* \s */
      {
         if (c == '\n')
            lineno++;
         switch (state)
         {
            case EATING_TYPE:
               if (in (&type, modifer_keywords, NMODIFIER_KEYWORDS))
               {
                  if (is (&type, "static"))
                  {
                     if (is_static != DEFAULT)
                     {
                        reset ("static in addition to static or automatic");
                        state = EATING_TYPE;
                     }
                     else
                        is_static = YES;
                  }
                  else if (is (&type, "automatic"))
                  {
                     if (is_static != DEFAULT)
                     {
                        reset ("automatic in addition to static or automatic");
                        state = EATING_TYPE;
                     }
                     else
                        is_static = NO;
                  }
                  if (type.n >= MAX_LEN)
                     reset ("Type name too long");
                  else
                     type.c[type.n++] = c;
               }
               else if (in (&type, compound_keywords, NCOMPOUND_KEYWORDS))
               {
                  state = COMPOUND_TYPE;
                  is_compound = 1;
               }
               else if (in (&type, nondecl_keywords, NNONDECL_KEYWORDS))
                  state = NONE;
               else
                  state = AFTER_TYPE;
               break;
            case EATING_NAME:
               state = AFTER_NAME;
               break;
            case NONE: case READY_FOR_DECL: case COMPOUND_TYPE:
            case AFTER_TYPE: case AFTER_NAME: case DID_BRACKET:
            case AFTER_COMMA:
         }
      }
      else if (c == ';')
      {
         if (state == EATING_NAME || state == AFTER_NAME ||
               state == DID_BRACKET)
         {
            int i, j;
            char *prefix;
            if (is_static == DEFAULT)
               prefix = "";
            else if (is_static == YES)
               prefix = " [static]";
            else /* if (is_static == NO) */
               prefix = " [auto]";
            type.c[type.n] = '\0';
            printf ("[level %d]%s %s", bracelevel, prefix, type.c);
            for (i = 0; i < nentry; i++)
            {
               entries[i].name.c[entries[i].name.n] = '\0';
               if (i > 0)
                  printf (",");
               printf (" %s", entries[i].name.c);
               for (j = 0; j < entries[i].nbrackets; j++)
               {
                  entries[i].brackets[j].c[entries[i].brackets[j].n] = '\0';
                  printf ("[%s]", entries[i].brackets[j].c);
               }
               printf (";\n");
            }
         }
         state = READY_FOR_DECL;
      }
      else if (c == '{')
      {
         if (in (&type, compound_keywords, NCOMPOUND_KEYWORDS))
               {
                  state = COMPOUND_TYPE;
                  is_compound = 1;
                  break;
               }
         if ((state == EATING_TYPE || state == AFTER_TYPE ||
               state == COMPOUND_TYPE) &&
             (is_compound || in (&type, compound_keywords, NCOMPOUND_KEYWORDS)))
         {
            if (read_until_inbalance (&block) == '}')
               state = AFTER_TYPE;
            else
               reset ("Got lost while looking for matching }");
         }
         else
         {
            bracelevel++;
            state = READY_FOR_DECL;
         }
      }
      else if (c == '}')
      {
         bracelevel--;
         if (bracelevel < 0)
            reset ("} without matching {");
         state = READY_FOR_DECL;
      }
      else if (c == ':')
      {
         state = READY_FOR_DECL;
      }
      else if (c == '[')
      {
         if (state == EATING_NAME || state == AFTER_NAME ||
               state == DID_BRACKET)
         {
            if (currentry->nbrackets >= MAX_BRACKETS)
               reset ("Too many []s in declaration");
            else if (read_until_inbalance_small
                  (&(currentry->brackets[currentry->nbrackets++])) == ']')
               state = DID_BRACKET;
            else
               reset ("Got lost while looking for matching ]");
         }
         else
            state = NONE;
      }
      else
         state = NONE;
   }
}

int read_until_inbalance_small (char_array *dest)
{
   int parens = 0, brackets = 0, braces = 0;
   int c, done = 0;

   dest->n = 0;
   while ((c = fgetc (stdin)) != EOF)
   {
      if (dest->n < MAX_LEN)
         dest->c[dest->n++] = c;
      else
         return -1;
      switch (c)
      {
         case '(': parens++; break;
         case ')': if (parens-- <= 0) done = 1; break;
         case '[': brackets++; break;
         case ']': if (brackets-- <= 0) done = 1; break;
         case '{': braces++; break;
         case '}': if (braces-- <= 0) done = 1; break;
         case '\n': lineno++; break;
      }
      if (done)
      {
         dest->n--;
         break;
      }
   }
   return c;
}

int read_until_inbalance (big_char_array *dest)
{
   int parens = 0, brackets = 0, braces = 0;
   int c, done = 0, escaping, squote = 0, dquote = 0;

   dest->n = 0;
   while ((c = fgetc (stdin)) != EOF)
   {
      if (dest->n < BIG_LEN)
         dest->c[dest->n++] = c;
      else
         return -1;
      if (c == '\\')
         escaping = 1 - escaping;
      else
         escaping = 0;
      switch (c)
      {
         case '(': parens++; break;
         case ')': if (parens-- <= 0) done = 1; break;
         case '[': brackets++; break;
         case ']': if (brackets-- <= 0) done = 1; break;
         case '{': braces++; break;
         case '}': if (braces-- <= 0) done = 1; break;
         case '\n': lineno++; break;
         case '\'': if (!escaping && !dquote) squote = 1 - squote; break;
         case '\"': if (!escaping && !squote) dquote = 1 - dquote; break;
      }
      if (done)
      {
         dest->n--;
         break;
      }
   }
   return c;
}

int is (char_array *s, char *s2)
{
   s->c[s->n] = '\0';
   return !strcmp (s->c, s2);
}

int in (char_array *s, char **list, int n)
{
   int i;
   for (i = 0; i < n; i++)
      if (is (s, list[i]))
         return 1;
   return 0;
}

