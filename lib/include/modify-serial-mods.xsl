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
               xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-4.xsd"
               xmlns:flvc="info:flvc/manifest/v1">
      <xsl:apply-templates select="node()[normalize-space()]|@*[normalize-space()]"/>
	  <xsl:call-template name="titleParse"/>
    </mods:mods>
  </xsl:template>

  <xsl:template
      match="*[not(node())] | *[not(node()[2]) and node()/self::text() and not(normalize-space())]"/>

  <xsl:template match="mods:titleInfo[@type='alternative']">
	<titleInfo type="alternative">
	  <title><xsl:value-of select="."/></title>
	</titleInfo>
  </xsl:template>

  <xsl:template match="mods:titleInfo/mods:title">
	<title><xsl:value-of select="substring-before(.,'.')"/></title>
  </xsl:template>

  <xsl:template name="titleParse">
	<relatedItem>
	  <part>
		<detail type="title">
		  <title><xsl:value-of select="substring-before(substring-after(mods:titleInfo/mods:title,'1. '),'.')"/><xsl:value-of select="substring-before(substring-after(mods:titleInfo/mods:title,'2. '),'.')"/><xsl:value-of select="substring-before(substring-after(mods:titleInfo/mods:title,'3. '),'.')"/><xsl:value-of select="substring-before(substring-after(mods:titleInfo/mods:title,'4. '),'.')"/></title>
		</detail>
		<detail type="issue">
		  <caption>Issue</caption>
		  <number><xsl:value-of select="substring(substring-after(mods:titleInfo/mods:title,'Issue '),1,2)"/></number>
		</detail>
		<detail type="volume">
		  <caption>Volume</caption>
		  <number><xsl:value-of select="substring(substring-after(mods:titleInfo/mods:title,'Volume '),1,2)"/></number>
		</detail>
	  </part>
	</relatedItem>
	<!-- <originInfo>
		 <dateIssued><xsl:value-of select="substring-after(text(),', ')"/></dateIssued>
		 </originInfo> -->
  </xsl:template>

  <xsl:template match="mods:location[@displayLabel='purl']"/>

</xsl:stylesheet>
