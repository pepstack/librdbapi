﻿
/*
 * sd_xplatform.c
 *
 * See the COPYING file for the terms of usage and distribution.
 */

#include <stdio.h>
#include <string.h>
#include <log4c/defs.h>



#include <xplatform/xplatform.h>

/****************** getopt *******************************/

#define	EOF	(-1)

 int sd_opterr = 1;
 int sd_optind = 1;
 int sd_optopt = 0;
 char *sd_optarg = NULL;
 int _sp = 1;

#define warn(a,b,c)fprintf(stderr,a,b,c)

 void
 getopt_reset(void)
 {
 	sd_opterr = 1;
 	sd_optind = 1;
 	sd_optopt = 0;
 	sd_optarg = NULL;
 	_sp = 1;
 }

 int
 sd_getopt(int argc, char *const *argv, const char *opts)
 {
 	char c;
 	char *cp;

 	if (_sp == 1) {
 		if (sd_optind >= argc || argv[sd_optind][0] != '-' ||
 		    argv[sd_optind] == NULL || argv[sd_optind][1] == '\0')
 			return (EOF);
 		else if (strcmp(argv[sd_optind], "--") == 0) {
 			sd_optind++;
 			return (EOF);
 		}
 	}
 	sd_optopt = c = (unsigned char)argv[sd_optind][_sp];
 	if (c == ':' || (cp = strchr(opts, c)) == NULL) {
 		if (opts[0] != ':')
 			warn("%s: illegal option -- %c\n", argv[0], c);
 		if (argv[sd_optind][++_sp] == '\0') {
 			sd_optind++;
 			_sp = 1;
 		}
 		return ('?');
 	}

 	if (*(cp + 1) == ':') {
 		if (argv[sd_optind][_sp+1] != '\0')
 			sd_optarg = &argv[sd_optind++][_sp+1];
 		else if (++sd_optind >= argc) {
 			if (opts[0] != ':') {
 				warn("%s: option requires an argument"
 				    " -- %c\n", argv[0], c);
 			}
 			_sp = 1;
 			sd_optarg = NULL;
 			return (opts[0] == ':' ? ':' : '?');
 		} else
 			sd_optarg = argv[sd_optind++];
 		_sp = 1;
 	} else {
 		if (argv[sd_optind][++_sp] == '\0') {
 			_sp = 1;
 			sd_optind++;
 		}
 		sd_optarg = NULL;
 	}
 	return (c);
 }

/*
 * Placeholder for WIN32 version to get last changetime of a file
 */
#ifdef _WIN32
int sd_stat_ctime(const char* path, time_t* time)
{
    return -1;
}
#else
#include <sys/stat.h>
int sd_stat_ctime(const char* path, time_t* time)
{
	struct stat astat;
	int statret=stat(path,&astat);
	if (0 != statret)
	{
		return statret;
	}
	*time=astat.st_ctime;
	return statret;
}
#endif

#ifdef DEFAULT_XP_FILE_EXIST
#include <sys/stat.h>
int xp_file_exist(const char* name)
{
	struct stat	info;
	return stat(name,&info) == 0;
}
#endif //DEFAULT_XP_FILE_EXIST

#ifdef DEFAULT_XP_GETTIMEOFDAY
void xp_gettimeofday(log4c_common_time_t* p,void* reserve)
{
	struct tm tm;
	time_t t = time(0);
	tm = *localtime(&t);

	p->tm_hour	= tm.tm_hour;
	p->tm_isdst	= tm.tm_isdst;
	p->tm_mday	= tm.tm_mday;
	p->tm_milli	= 0;
	p->tm_min	= tm.tm_min;
	p->tm_mon	= tm.tm_mon;
	p->tm_origin= 0;
	p->tm_sec	= tm.tm_sec;
	p->tm_wday	= tm.tm_wday;
	p->tm_yday	= tm.tm_yday;
	p->tm_year	= tm.tm_year + 1900;
}
#endif //DEFAULT_XP_GETTIMEOFDAY
