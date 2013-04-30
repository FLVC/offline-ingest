<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:mods="http://www.loc.gov/mods/v3" exclude-result-prefixes="mods"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <!-- After LOC's MODS to DC -->

  <xsl:output method="text" indent="no" omit-xml-declaration="yes"/>
  <xsl:strip-space elements="*"/>

  <xsl:template match="/">
    <xsl:choose>
      <xsl:when test="//mods:modsCollection">
        <xsl:apply-templates/>
        <xsl:for-each select="mods:modsCollection/mods:mods">
          <xsl:apply-templates/>
        </xsl:for-each>
      </xsl:when>
      <xsl:otherwise>
        <xsl:for-each select="mods:mods">
          <xsl:apply-templates/>
        </xsl:for-each>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="mods:titleInfo">
    <xsl:value-of select="mods:nonSort"/>
    <xsl:if test="mods:nonSort">
      <xsl:text> </xsl:text>
    </xsl:if>
    <xsl:value-of select="mods:title"/>
    <xsl:if test="mods:subTitle">
      <xsl:text>: </xsl:text>
      <xsl:value-of select="mods:subTitle"/>
    </xsl:if>
    <xsl:if test="mods:partNumber">
      <xsl:text>. </xsl:text>
      <xsl:value-of select="mods:partNumber"/>
    </xsl:if>
    <xsl:if test="mods:partName">
      <xsl:text>. </xsl:text>
      <xsl:value-of select="mods:partName"/>
    </xsl:if>
  </xsl:template>

  <!-- suppress all else:-->
  <xsl:template match="*"/>

</xsl:stylesheet>
