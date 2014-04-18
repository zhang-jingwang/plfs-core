# - Try to find mchecksum
# Once done this will define
#  MCHECKSUM_FOUND - System has mchecksum
#  MCHECKSUM_INCLUDE_DIRS - The mchecksum include directories
#  MCHECKSUM_LIBRARIES - The libraries needed to use mchecksum
#  MCHECKSUM_DEFINITIONS - Compiler switches required for using mchecksum

find_package(PkgConfig)
pkg_check_modules(PC_LIBMCHECKSUM QUIET libmchecksum)
set(MCHECKSUM_DEFINITIONS ${PC_LIBMCHECKSUM_CFLAGS_OTHER})

#

find_path(MCHECKSUM_INCLUDE_DIR mchecksum.h mchecksum.h  mchecksum_config.h  mchecksum_error.h
          HINTS ${PC_LIBMCHECKSUM_INCLUDEDIR} ${PC_LIBMCHECKSUM_INCLUDE_DIRS}
          PATH_SUFFIXES mchecksum )

find_library(MCHECKSUM_LIBRARY NAMES mchecksum
             HINTS ${PC_LIBMCHECKSUM_LIBDIR} ${PC_LIBMCHECKSUM_LIBRARY_DIRS} )

set(MCHECKSUM_LIBRARIES ${MCHECKSUM_LIBRARY} )
set(MCHECKSUM_INCLUDE_DIRS ${MCHECKSUM_INCLUDE_DIR} )

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set MCHECKSUM_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(MCHECKSUM "${FAIL_MSG}"
                                   MCHECKSUM_LIBRARY MCHECKSUM_INCLUDE_DIR)
mark_as_advanced(MCHECKSUM_INCLUDE_DIR MCHECKSUM_LIBRARY )
