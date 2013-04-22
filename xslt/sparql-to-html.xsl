<?xml version="1.0" encoding="iso8859-1"?>

<!--

  XSLT script to format SPARQL Variable Results XML Format as xhtml

  Copyright © 2004 World Wide Web Consortium, (Massachusetts
  Institute of Technology, European Research Consortium for
  Informatics and Mathematics, Keio University). All Rights
  Reserved. This work is distributed under the W3C® Software
  License [1] in the hope that it will be useful, but WITHOUT ANY
  WARRANTY; without even the implied warranty of MERCHANTABILITY or
  FITNESS FOR A PARTICULAR PURPOSE.

  [1] http://www.w3.org/Consortium/Legal/2002/copyright-software-20021231

-->

<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns="http://www.w3.org/1999/xhtml"
  xmlns:res="http://www.w3.org/2001/sw/DataAccess/rf1/result"
  exclude-result-prefixes="res xsl">

  <!--
  <xsl:output
    method="html"
    media-type="text/html"
    doctype-public="-//W3C//DTD HTML 4.01 Transitional//EN"
    indent="yes"
    encoding="UTF-8"/>
  -->

  <!-- or this? -->

  <xsl:output
    method="xml" 
    indent="yes"
    encoding="UTF-8" 
    doctype-public="-//W3C//DTD XHTML 1.0 Strict//EN"
    doctype-system="http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"
    omit-xml-declaration="no" />


  <xsl:template match="res:sparql">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
  <head>
    <title>SPARQL Variable Binding Results to XHTML table (XSLT)</title>
  </head>
  <body>

    <h1>SPARQL Variable Binding Results to XHTML table (XSLT)</h1>

    <table border="1">
<xsl:text>
    </xsl:text>
<tr>
  <xsl:for-each select="res:head/*">
    <th><xsl:value-of select="@name"/></th>
  </xsl:for-each>
</tr>
<xsl:text>
</xsl:text>

  <xsl:for-each select="res:results/res:result"> 
<xsl:text>
    </xsl:text>
<tr>
<xsl:text>
    </xsl:text>
    <xsl:call-template name="result" />
</tr>
<xsl:text>
    </xsl:text>
  </xsl:for-each>

    </table>


  </body>
</html>

  </xsl:template>


  <xsl:template name="result">
    <xsl:for-each select="./*"> 
     <xsl:variable name="name" select="local-name()" />
     <xsl:text>
      </xsl:text>
     <td>
	<xsl:choose>
	  <xsl:when test="@bnodeid">
	    <!-- blank node value -->
	    <xsl:text>nodeID </xsl:text>
	    <xsl:value-of select="@bnodeid"/>
	  </xsl:when>
	  <xsl:when test="@uri">
	    <!-- URI value -->
	    <xsl:variable name="uri" select="@uri"/>
	    <xsl:text>URI </xsl:text>
	    <xsl:value-of select="$uri"/>
	  </xsl:when>
	  <xsl:when test="@datatype">
	    <!-- datatyped literal value -->
	    <xsl:value-of select="text()"/> (datatype <xsl:value-of select="@datatype"/> )
	  </xsl:when>
	  <xsl:when test="@xml:lang">
	    <!-- lang-string -->
	    <xsl:value-of select="text()"/> @ <xsl:value-of select="@xml:lang"/>
	  </xsl:when>
	  <xsl:when test="@bound='false'">
	    <!-- unbound -->
	    [unbound]
	  </xsl:when>
	  <xsl:when test="string-length(text()) != 0">
	    <!-- present and not empty -->
	    <xsl:value-of select="text()"/>
	  </xsl:when>
	  <xsl:when test="string-length(text()) = 0">
	    <!-- present and empty -->
            [empty]
	  </xsl:when>
	  <xsl:otherwise>
	    [unbound]
	  </xsl:otherwise>
	</xsl:choose>
     </td>
    <xsl:text>
</xsl:text>
    </xsl:for-each>
  </xsl:template>

</xsl:stylesheet>
