<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns="http://www.loc.gov/mods/v3"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:mods="http://www.loc.gov/mods/v3"
                xmlns:flvc="info:flvc/manifest/v1"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:dcterms="http://purl.org/dc/terms/"
                exclude-result-prefixes="mods dc marc">


  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" media-type="text/xml"/>
  <xsl:strip-space elements="*"/>

  <xsl:template match="node()|@*">
    <xsl:copy>
      <xsl:apply-templates select="node()[normalize-space()]|@*[normalize-space()]"/>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="mods:mods">
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3" xmlns="http://www.loc.gov/mods/v3"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xlink="http://www.w3.org/1999/xlink"
               xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd"
               xmlns:flvc="info:flvc/manifest/v1">
      <xsl:apply-templates select="node()[normalize-space()]|@*[normalize-space()]"/>
      <xsl:call-template name="origin"/>
      <xsl:call-template name="titleParse"/>
    </mods:mods>

  </xsl:template>

  <xsl:template
     match="*[not(node())] | *[not(node()[2]) and node()/self::text() and not(normalize-space())]"/>

  <xsl:template match="mods:titleInfo/mods:title">
	<title><xsl:value-of select="substring-before(.,', ')"/></title>
  </xsl:template>

  <xsl:template name="titleParse">
  	<relatedItem type="host">
  	  <part>
  		<detail type="title">
  		  <title><xsl:value-of select="substring-after(mods:titleInfo/mods:title,', ')"/></title>
  		</detail>
  	  </part>
  	</relatedItem>
  </xsl:template>

  <xsl:template name="origin">
	<originInfo>
	  <dateIssued><xsl:value-of select="substring-after(substring-after(mods:titleInfo/mods:title,', '),', ')"/><xsl:text>-</xsl:text>  <xsl:choose>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'January')">
			<xsl:text>01</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'February')">
			<xsl:text>02</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'March')">
			<xsl:text>03</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'April')">
			<xsl:text>04</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'May')">
			<xsl:text>05</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'June')">
			<xsl:text>06</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'July')">
			<xsl:text>07</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'August')">
			<xsl:text>08</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'September')">
			<xsl:text>09</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'October')">
			<xsl:text>10</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'November')">
			<xsl:text>11</xsl:text></xsl:when>
		  <xsl:when test="contains(mods:titleInfo/mods:title, 'December')">
			<xsl:text>12</xsl:text></xsl:when><xsl:otherwise><xsl:text/></xsl:otherwise></xsl:choose>
		<xsl:text>-</xsl:text><xsl:value-of select="substring-before(substring-after(substring-after(mods:titleInfo/mods:title,', '),' '),',')"/></dateIssued>
	</originInfo>
  </xsl:template>

  <xsl:template match="mods:location[@displayLabel='purl']"/>

  <xsl:template match="mods:mods">
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3" xmlns="http://www.loc.gov/mods/v3"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xlink="http://www.w3.org/1999/xlink"
               xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd"
               xmlns:flvc="info:flvc/manifest/v1">
      <xsl:apply-templates select="node()[normalize-space()]|@*[normalize-space()]"/>
    </mods:mods>

  </xsl:template>

  <xsl:template
     match="*[not(node())] | *[not(node()[2]) and node()/self::text() and not(normalize-space())]"/>


  <xsl:template match="mods:originInfo/mods:dateIssued[@*]/">
    <xsl:value-of select="substring-after(//mods:titleInfo/mods:title,', ')"/><xsl:text>-</xsl:text>  <xsl:choose>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'January')">
        <xsl:text>01</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'February')">
        <xsl:text>02</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'March')">
        <xsl:text>03</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'April')">
        <xsl:text>04</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'May')">
        <xsl:text>05</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'June')">
        <xsl:text>06</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'July')">
        <xsl:text>07</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'August')">
        <xsl:text>08</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'September')">
        <xsl:text>09</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'October')">
        <xsl:text>10</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'November')">
        <xsl:text>11</xsl:text></xsl:when>
      <xsl:when test="contains(//mods:titleInfo/mods:title, 'December')">
        <xsl:text>12</xsl:text></xsl:when><xsl:otherwise><xsl:text/></xsl:otherwise></xsl:choose>
    <xsl:text>-</xsl:text><xsl:value-of select="substring-after(substring-before(//mods:titleInfo/mods:title,','),' ')"/>
  </xsl:template>


  <xsl:template match="//mods:genre"><genre><xsl:text>newspaper</xsl:text></genre></xsl:template>
  <xsl:template match="mods:location[@displayLabel='purl']"/>
</xsl:stylesheet>
