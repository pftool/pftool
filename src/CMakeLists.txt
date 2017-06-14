
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# include the sub directories with libraries
#add_subdirectory(serial)
#add_subdirectory(smpi)
#add_subdirectory(path)
#add_subdirectory(task)

# create the executable
add_executable(
    pftool
    debug.h cta.c ctf.c ctm.c ctm.h hashtbl.c hashtbl.h hashdataCTM.c
    hashdataCTM.h str.c str.h pfutils.cpp pfutils.h pftool.cpp pftool.h Path.cpp
    Path.h
    )
#target_link_libraries(mfagent path smpi task)
install (TARGETS pftool DESTINATION bin)

# MPI stuff
find_package(MPI REQUIRED)
target_link_libraries(pftool ${MPI_LIBRARIES})
if(MPI_COMPILE_FLAGS)
  set_target_properties(pftool PROPERTIES
    COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
endif()
if(MPI_LINK_FLAGS)
  set_target_properties(pftool PROPERTIES
    LINK_FLAGS "${MPI_LINK_FLAGS}")
endif()

