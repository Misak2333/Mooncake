# SPDX-License-Identifier: Apache-2.0

# Relative install directories follow --install --prefix and package relocation.
# Absolute destinations retain the configured paths, as CMake install does.
function(conductor_configure_pkg_config_file TEMPLATE OUTPUT LIBDIR INCLUDEDIR)
  get_filename_component(CONDUCTOR_PC_PREFIX "${CMAKE_INSTALL_PREFIX}" ABSOLUTE)
  if(IS_ABSOLUTE "${LIBDIR}")
    set(CONDUCTOR_PC_LIBDIR "${LIBDIR}")
  else()
    # Multiarch libdirs require more than two steps back to the prefix.
    file(RELATIVE_PATH pkgconfig_to_prefix
         "${CONDUCTOR_PC_PREFIX}/${LIBDIR}/pkgconfig" "${CONDUCTOR_PC_PREFIX}")
    set(CONDUCTOR_PC_PREFIX "\${pcfiledir}/${pkgconfig_to_prefix}")
    set(CONDUCTOR_PC_LIBDIR "\${prefix}/${LIBDIR}")
  endif()
  if(IS_ABSOLUTE "${INCLUDEDIR}")
    set(CONDUCTOR_PC_INCLUDEDIR "${INCLUDEDIR}")
  else()
    set(CONDUCTOR_PC_INCLUDEDIR "\${prefix}/${INCLUDEDIR}")
  endif()

  configure_file("${TEMPLATE}" "${OUTPUT}" @ONLY)
endfunction()
